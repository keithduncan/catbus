use std::{
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
  fs::File,
  collections::BTreeMap,
};

use tarball_codec;

extern crate tar;

extern crate libflate;
use self::libflate::gzip;

extern crate libc;

extern crate rayon;
use self::rayon::prelude::*;

#[derive(Clone)]
enum ArchiveEntry {
  Concrete {
    header: tar::Header,
    path: PathBuf,
    bytes: Vec<u8>,
  },
  Lookup {
    header: tar::Header,
    path: PathBuf,
    digest: Vec<u8>,
  },
}

fn find_entries(wanted: &BTreeMap<PathBuf, Vec<u8>>, candidate: &Path, candidate_index: &Path) -> Vec<(PathBuf, ArchiveEntry)> {
  Vec::new()
}

fn merge_entries(entries: Vec<ArchiveEntry>, lookup: BTreeMap<PathBuf, ArchiveEntry>) -> Vec<ArchiveEntry> {
  entries
    .into_iter()
    .map(|element| {
      match element {
        ArchiveEntry::Lookup { header, path, digest } => {
          lookup
            .get(&path)
            .cloned()
            .unwrap_or(ArchiveEntry::Lookup { header, path, digest })
        }
        e => e
      }
    })
    .collect()
}

// Search a directory for pairs of indexes and tarballs
fn discover_indexes(dir: &Path) -> Vec<(PathBuf, PathBuf)> {
  Vec::new()
}

fn request_remaining_entries(archive_entries: &[ArchiveEntry]) -> io::Result<()> {
  let mut stdout = BufWriter::new(io::stdout());

  archive_entries
    .iter()
    .map(|entry| {
      match entry {
        &ArchiveEntry::Lookup { ref header, ref path, ref digest } => {
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

fn serialise_entries_to_writer<T: Write>(archive_entries: Vec<ArchiveEntry>, write: T) -> io::Result<()> {
  let mut output_builder = tar::Builder::new(write);

  archive_entries
    .into_iter()
    .map(|entry| {
      match entry {
        ArchiveEntry::Concrete { mut header, path, bytes } => {
          output_builder.append_data(&mut header, path, bytes.as_slice())
        }
        _ => Err(io::Error::new(io::ErrorKind::Other, "non concrete entry"))
      }
    })
    .collect::<io::Result<Vec<()>>>()?;

  let mut output = output_builder.into_inner()?;
  output.flush()
}

fn finalise_output(archive_entries: Vec<ArchiveEntry>, output_path: &Path, index: &[u8], index_path: &Path) -> io::Result<()> {
  let output_file = File::create(output_path)?;
  eprintln!("[receive-index] writing output tarball");
  serialise_entries_to_writer(archive_entries, output_file)?;

  let mut index_file = File::create(index_path)?;
  eprintln!("[receive-index] writing index tarball");
  index_file.write_all(index)
}

fn read_index<T: Read>(read: &mut BufReader<T>) -> io::Result<(Vec<u8>, Vec<ArchiveEntry>, BTreeMap<PathBuf, Vec<u8>>)> {
  // Read the index
  eprintln!("[receive-index] receiving index tarball");
  let index = tarball_codec::read("[receive-index]", read)?;

  // Collect the list of lookup
  let mut want_list = BTreeMap::new();

  let archive_entries: Vec<ArchiveEntry> = {
    // The index is always compressed
    let decoder = gzip::Decoder::new(index.as_slice())?;
    let mut index_archive = tar::Archive::new(decoder);

    index_archive
      .entries()?
      .map(|entry| {
        let mut entry = entry?;

        let path = entry.path()?.to_path_buf();
        let header = entry.header().clone();

        let mut content = Vec::new();
        entry.read_to_end(&mut content)?;

        if header.entry_type().is_file() {
          want_list.insert(path.clone(), content.clone());

          Ok(ArchiveEntry::Lookup {
            header: header,
            path: path,
            digest: content,
          })
        } else {
          Ok(ArchiveEntry::Concrete {
            header: header,
            path: path,
            bytes: content,
          })
        }
      })
      .collect::<io::Result<Vec<ArchiveEntry>>>()?
  };

  Ok((index, archive_entries, want_list))
}

fn merge_local_entries(archive_entries: Vec<ArchiveEntry>, want_list: &BTreeMap<PathBuf, Vec<u8>>, destination_path: &Path) -> Vec<ArchiveEntry> {
  // Find adjacent indexes
  let indexes = discover_indexes(destination_path);

  // Find the wanted entries in the adjacent indexes
  let discovered_entries: BTreeMap<PathBuf, ArchiveEntry> = indexes
    .par_iter()
    .flat_map(|(index_path, tarball_path)| {
      find_entries(&want_list, index_path, tarball_path)
    })
    .collect();

  // Merge the found elements into the list of archive
  // elements
  merge_entries(archive_entries, discovered_entries)
}

pub fn receive_index(destination_path: &Path, destination_file: &str) -> io::Result<()> {
  let mut input = BufReader::new(io::stdin());

  let (index, archive_entries, want_list) = read_index(&mut input)?;

  // Start workers to scan the local library of parts
  let archive_entries = merge_local_entries(archive_entries, &want_list, destination_path);

  // Ask the sender for the remaining lookup parts
  request_remaining_entries(&archive_entries)?;
  eprintln!("[receive-index] receiving wanted tarball");
  // Read the tarball of wanted parts
  let want = tarball_codec::read("[receive-index]", &mut input)?;
  let mut want_archive = tar::Archive::new(want.as_slice());

  let want_archive_entries_by_path = want_archive
    .entries()?
    .map(|entry| {
      let mut entry = entry?;

      let path = entry.path()?.to_path_buf();
      let header = entry.header().clone();

      let mut content = Vec::new();
      entry.read_to_end(&mut content)?;

      Ok((path.clone(), ArchiveEntry::Concrete {
        header: header,
        path: path,
        bytes: content,
      }))
    })
    .collect::<io::Result<Vec<_>>>()?
    .into_iter()
    .collect::<BTreeMap<PathBuf, ArchiveEntry>>();

  // Merge the received elements into the list of archive
  // elements
  let archive_entries = merge_entries(archive_entries, want_archive_entries_by_path);

  // Translate the list of archive entries into an archive
  let mut output_path = PathBuf::from(destination_path);
  output_path.push(destination_file);
  let mut index_path = PathBuf::from(destination_path);
  index_path.push(format!("{}.idx", destination_file));
  finalise_output(archive_entries, &output_path, &index, &index_path)
}