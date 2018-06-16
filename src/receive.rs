use std::{
  path::{
    Path,
    PathBuf,
  },
  io::{
    self,
    BufReader,
    Read,
    Write,
  },
  fs::File,
};

use tarball_codec;

extern crate tar;

extern crate libflate;
use self::libflate::gzip;

extern crate libc;

pub fn receive_index(destination_path: &Path, destination_file: &str) -> io::Result<()> {
  let mut stdin = BufReader::new(io::stdin());
  let mut stdout = io::stdout();

  // Destination we're going to write a full tarball to
  let mut index_path = PathBuf::from(destination_path);
  index_path.push(format!("{}.idx", destination_file));

  let mut output_path = PathBuf::from(destination_path);
  output_path.push(destination_file);

  // Read the index
  eprintln!("[receive-index] receiving index tarball");
  let index = tarball_codec::read_tarball("[receive-index]", &mut stdin)?;

  // The index is always compressed
  let decoder = gzip::Decoder::new(index.as_slice())?;
  let mut index_archive = tar::Archive::new(decoder);

  let output_file = File::create(output_path)?;
  let mut output_builder = tar::Builder::new(output_file);

  for file in index_archive.entries()? {
    let mut file = file.expect("entry file");

    let mut new_header = file.header().clone();

    if new_header.entry_type() == tar::EntryType::Regular {
      let mut file_hash = Vec::new();
      file.read_to_end(&mut file_hash)?;

      let entry_path = file.path()?;
      // TODO handle error
      let entry_path = entry_path.to_str().expect("entry path");

      // Tell sender we want it
      eprintln!("[receive-index] sending want {:?} {:?}", new_header.entry_type(), entry_path);
      stdout.write_fmt(format_args!("{}\n", entry_path))?;
    } else {
      let entry_path = file.path().expect("entry path").into_owned();
      output_builder.append_data(&mut new_header, entry_path, file)?;
    }
  }

  // Tell the sender EOF so they send the want parts
  stdout.flush()?;
  unsafe {
    libc::close(1);
  }

  // Read the tarball of wanted parts
  eprintln!("[receive-index] receiving wanted tarball");
  let want = tarball_codec::read_tarball("[receive-index]", &mut stdin)?;

  // Append it to the archive we've built it
  let mut want_archive = tar::Archive::new(want.as_slice());
  for file in want_archive.entries()? {
    let mut file = file?;

    eprintln!("[receive-index] receiving {:?}", file.path()?);

    let mut new_header = file.header().clone();
    let entry_path = file.path()?.into_owned();
    output_builder.append_data(&mut new_header, entry_path, file)?;
  }

  eprintln!("[receive-index] writing output tarball");
  let mut output = output_builder.into_inner()?;
  output.flush()?;

  let mut index_file = File::create(index_path)?;
  eprintln!("[receive-index] writing index tarball");
  index_file.write_all(&index)?;

  Ok(())
}