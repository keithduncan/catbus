use std::{
  thread,
  time,
  path::{
    Path,
    PathBuf,
  },
  io::{
    self,
    BufReader,
    BufWriter,
    Read,
    Write,
  },
  fs::{
    self,
    File,
  },
  collections::{
    HashSet,
    HashMap,
  },
  borrow::Cow,
};

use tarball_codec;

extern crate tar;

extern crate libflate;
use self::libflate::gzip;

extern crate libc;

extern crate rayon;
use self::rayon::prelude::*;

type ContentDigest = Vec<u8>;

#[derive(Clone)]
enum ArchiveEntry<'a> {
  Concrete(Cow<'a, ConcreteEntry>),
  Lookup {
    header: tar::Header,
    path: PathBuf,
    digest: ContentDigest,
  },
}

#[derive(Clone)]
struct ConcreteEntry {
  header: tar::Header,
  path: PathBuf,
  bytes: Vec<u8>,
}

fn find_entries(wanted: &HashSet<(PathBuf, ContentDigest)>, candidate: &Path, candidate_index: &Path) -> io::Result<Vec<(PathBuf, ConcreteEntry)>> {
  let index = BufReader::new(File::open(candidate_index)?);
  let (_, want_list) = archive_entries_for_index(index)?;

  let extract_list: HashSet<PathBuf> = want_list
    .into_iter()
    .filter_map(|entry| {
      if wanted.contains(&entry) {
        Some(entry.0)
      } else {
        None
      }
    })
    .collect();

  if extract_list.len() == 0 {
    return Ok(Vec::new())
  }

  let archive = BufReader::new(File::open(candidate)?);
  let mut archive = tar::Archive::new(archive);

  let archive_entries = archive
    .entries()?
    .into_iter()
    .filter_map(|entry| {
      let mut entry = entry.ok()?;

      let path = entry.path().ok()?.to_path_buf();

      if extract_list.contains(&path) {
        let mut content = Vec::new();
        entry.read_to_end(&mut content).ok()?;

        Some((path.clone(), ConcreteEntry {
          header: entry.header().clone(),
          path: path,
          bytes: content,
        }))
      } else {
        None
      }
    })
    .collect();

  Ok(archive_entries)
}

fn merge_entries<'a>(entries: &mut Vec<ArchiveEntry<'a>>, lookup: &'a HashMap<PathBuf, ConcreteEntry>) -> usize {
  fn find_entry<'a>(element: &ArchiveEntry<'a>, lookup: &'a HashMap<PathBuf, ConcreteEntry>)
    -> Option<&'a ConcreteEntry>
  {
    match element {
      ArchiveEntry::Lookup { path, .. } => lookup.get(path),
      _ => None,
    }
  }

  let mut merged: usize = 0;

  for element in entries {
    if let Some(c) = find_entry(element, lookup) {
      merged += c.bytes.len();
      *element = ArchiveEntry::Concrete(Cow::Borrowed(c));
    }
  }

  merged
}

// Search a directory for pairs of indexes and tarballs
fn discover_indexes(dir: &Path) -> Vec<(PathBuf, PathBuf)> {
  fs::read_dir(dir)
    .map(|entries| {
      entries
        .filter_map(|entry| {
          entry.ok()
        })
        .filter_map(|entry| {
          let path = entry.path();

          if path.extension()?.to_str()? == "idx" {
            let tarball_path = path.with_extension("");
            Some((path, tarball_path))
          } else {
            None
          }
        })
        .collect()
    })
    .unwrap_or(Vec::new())
}

fn request_remaining_entries(archive_entries: &[ArchiveEntry]) -> io::Result<()> {
  let mut stdout = BufWriter::new(io::stdout());

  archive_entries
    .iter()
    .map(|entry| {
      match entry {
        &ArchiveEntry::Lookup { ref header, ref path, .. } => {
          // Tell sender we want it
          eprintln!("[receive-index] sending want {:?} {:?}", header.entry_type(), path);
          stdout.write_fmt(format_args!("{}\n", path.to_str().ok_or(io::Error::new(io::ErrorKind::Other, "non UTF8 path"))?))
        }
        _ => Ok(())
      }
    })
    .collect::<io::Result<Vec<_>>>()?;
  // Tell the sender EOF so they send the want parts
  stdout.flush()?;
  unsafe {
    libc::close(1);
  }

  Ok(())
}

fn serialise_entries_to_writer<T: Write>(archive_entries: Vec<Cow<ConcreteEntry>>, write: T) -> io::Result<()> {
  let mut output_builder = tar::Builder::new(write);

  archive_entries
    .into_iter()
    .map(|entry| {
      let mut header = entry.header.clone();
      output_builder.append_data(&mut header, &entry.path, entry.bytes.as_slice())
    })
    .collect::<io::Result<Vec<()>>>()?;

  output_builder.into_inner()?.flush()
}

fn finalise_output(archive_entries: Vec<Cow<ConcreteEntry>>, output_path: &Path, index: &[u8], index_path: &Path) -> io::Result<()> {
  let output_file = BufWriter::new(File::create(output_path)?);
  eprintln!("[receive-index] writing output tarball");
  serialise_entries_to_writer(archive_entries, output_file)?;

  let mut index_file = File::create(index_path)?;
  eprintln!("[receive-index] writing index tarball");
  index_file.write_all(index)
}

fn archive_entries_for_index<'a, T: Read>(read: T) -> io::Result<(Vec<ArchiveEntry<'a>>, HashSet<(PathBuf, ContentDigest)>)> {
  let mut want_list = HashSet::new();

  // An index is always compressed
  let decoder = gzip::Decoder::new(read)?;
  let mut index_archive = tar::Archive::new(decoder);

  let archive_entries = index_archive
    .entries()?
    .map(|entry| {
      let mut entry = entry?;

      let path = entry.path()?.to_path_buf();
      let header = entry.header().clone();

      let mut content = Vec::new();
      entry.read_to_end(&mut content)?;

      if header.entry_type().is_file() {
        want_list.insert((path.clone(), content.clone()));

        Ok(ArchiveEntry::Lookup {
          header: header,
          path: path,
          digest: content,
        })
      } else {
        Ok(ArchiveEntry::Concrete(Cow::Owned(ConcreteEntry {
          header: header,
          path: path,
          bytes: content,
        })))
      }
    })
    .collect::<io::Result<Vec<ArchiveEntry>>>()?;

  Ok((archive_entries, want_list))
}

fn read_remote_index<'a, 'b, T: Read>(read: &'a mut BufReader<T>) -> io::Result<(Vec<u8>, Vec<ArchiveEntry<'b>>, HashSet<(PathBuf, ContentDigest)>)> {
  // Read the index
  eprintln!("[receive-index] receiving index tarball");
  let index = tarball_codec::read("[receive-index]", read)?;

  let (archive_entries, want_list) = archive_entries_for_index(index.as_slice())?;

  Ok((index, archive_entries, want_list))
}

fn find_local_entries(want_list: &HashSet<(PathBuf, ContentDigest)>, destination_path: &Path) -> HashMap<PathBuf, ConcreteEntry> {
  // Find adjacent indexes
  let indexes = discover_indexes(destination_path);
  eprintln!("[receive-index] discover_indexes {:#?}", indexes);

  // Find the wanted entries in the adjacent indexes
  indexes
    .par_iter()
    .flat_map(|(index_path, tarball_path)| {
      let now = time::Instant::now();
      eprintln!("thread id {:?}, now {:?}, index {:?}", thread::current().id(), now, index_path);

      let entries = find_entries(&want_list, tarball_path, index_path)
        .map_err(|e| {
          eprintln!("discover_error {:#?}", e)
        })
        .unwrap_or(Vec::new());

      eprintln!("thread id {:?}, elapsed {:?}", thread::current().id(), now.elapsed());

      entries
    })
    // PERF this is taking 1.2s on the sample data to extend these into a useful collection
    .collect::<HashMap<PathBuf, ConcreteEntry>>()
}

fn find_remote_entries<T: Read>(archive_entries: &[ArchiveEntry], input: &mut BufReader<T>) -> io::Result<HashMap<PathBuf, ConcreteEntry>> {
  request_remaining_entries(&archive_entries)?;
  eprintln!("[receive-index] receiving wanted tarball");
  // Read the tarball of wanted parts
  let want = tarball_codec::read("[receive-index]", input)?;
  let mut want_archive = tar::Archive::new(want.as_slice());

  Ok(want_archive
    .entries()?
    .map(|entry| {
      let mut entry = entry?;

      let path = entry.path()?.to_path_buf();
      let header = entry.header().clone();

      let mut content = Vec::new();
      entry.read_to_end(&mut content)?;

      Ok((path.clone(), ConcreteEntry {
        header: header,
        path: path,
        bytes: content,
      }))
    })
    .collect::<io::Result<Vec<_>>>()?
    .into_iter()
    .collect::<HashMap<PathBuf, ConcreteEntry>>())
}

pub fn receive_index(destination_path: &Path, destination_file: &str) -> io::Result<()> {
  let mut input = BufReader::new(io::stdin());

  let local_entries;
  let remote_entries;
  let (index, mut archive_entries, want_list) = read_remote_index(&mut input)?;

  // Start workers to scan the local library of parts
  local_entries = find_local_entries(&want_list, destination_path);
  let merged_locally = merge_entries(&mut archive_entries, &local_entries);
  eprintln!("[receive-index] merged {:?} bytes from local parts", merged_locally);

  // Ask the sender for the remaining lookup parts
  remote_entries = find_remote_entries(&archive_entries, &mut input)?;
  let merged_remotely = merge_entries(&mut archive_entries, &remote_entries);
  eprintln!("[receive-index] merged {:?} bytes from remote parts", merged_remotely);

  // Ensure all entries are concrete
  let archive_entries = archive_entries
    .into_iter()
    .map(|entry| {
      match entry {
        ArchiveEntry::Concrete(c) => Ok(c),
        _ => Err(io::Error::new(io::ErrorKind::Other, "non concrete entry")),
      }
    })
    .collect::<io::Result<Vec<Cow<ConcreteEntry>>>>()?;

  // Translate the list of archive entries into an archive
  let mut output_path = PathBuf::from(destination_path);
  output_path.push(destination_file);
  let mut index_path = PathBuf::from(destination_path);
  index_path.push(format!("{}.idx", destination_file));

  finalise_output(archive_entries, &output_path, &index, &index_path)
}