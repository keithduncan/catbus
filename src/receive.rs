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
  let index = tarball_codec::read_tarball("[receive-index]", &mut stdin).expect("read index from upload-index");

  // The index is always compressed
  let decoder = gzip::Decoder::new(index.as_slice()).expect("gzip decoder");
  let mut index_archive = tar::Archive::new(decoder);

  let output_file = File::create(output_path).expect("create output file");
  let mut output_builder = tar::Builder::new(output_file);

  for file in index_archive.entries().expect("entries") {
    let mut file = file.expect("entry file");

    let mut new_header = file.header().clone();

    if new_header.entry_type() == tar::EntryType::Regular {
      let mut file_hash = Vec::new();
      file.read_to_end(&mut file_hash).expect("read entry content");

      let entry_path = file.path().expect("entry path");
      let entry_path = entry_path.to_str().expect("to str");

      // Tell sender we want it
      eprintln!("[receive-index] sending want {:?} {:?}", new_header.entry_type(), entry_path);
      stdout.write_fmt(format_args!("{}\n", entry_path)).expect("write wanted entry");
    } else {
      let entry_path = file.path().expect("entry path").into_owned();
      output_builder.append_data(&mut new_header, entry_path, file).expect("append entry to output");
    }
  }

  // Tell the sender EOF so they send the want parts
  stdout.flush().expect("flush wanted entries");
  unsafe {
    libc::close(1);
  }

  // Read the tarball of wanted parts
  eprintln!("[receive-index] receiving wanted tarball");
  let want = tarball_codec::read_tarball("[receive-index]", &mut stdin).expect("read wanted from upload-index");

  // Append it to the archive we've built it
  let mut want_archive = tar::Archive::new(want.as_slice());
  for file in want_archive.entries().expect("entries") {
    let mut file = file.expect("wanted entry");

    eprintln!("[receive-index] receiving {:?}", file.path().expect("entry path"));

    let mut new_header = file.header().clone();
    let entry_path = file.path().expect("entry path").into_owned();
    output_builder.append_data(&mut new_header, entry_path, file).expect("append wanted entry");
  }

  eprintln!("[receive-index] writing output tarball");
  let mut output = output_builder.into_inner().expect("write output");
  output.flush().expect("flush output");

  let mut index_file = File::create(index_path).expect("create index file");
  eprintln!("[receive-index] writing index tarball");
  index_file.write_all(&index).expect("write index to destination");

  Ok(())
}