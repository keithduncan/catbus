use std::{
  path::PathBuf,
  io::{
    self,
    BufRead,
    BufReader,
  },
  fs::File,
  collections::BTreeSet,
};

use tarball_codec;

extern crate tar;

extern crate libc;

pub fn upload_index(tar_path: &str, index_path: &str) -> io::Result<()> {
  // Output on stdout
  let mut stdout = io::stdout();

  // Send the index first
  eprintln!("[upload-index] sending index tarball");
  let mut index_file = File::open(index_path).expect("index file present");
  tarball_codec::write_tarball("[upload-index]", &mut index_file, &mut stdout).expect("write index to receive-index");

  let mut want_list = BTreeSet::new();

  // Wait to read requested parts on stdin
  eprintln!("[upload-index] reading want lines");
  let stdin = BufReader::new(io::stdin());
  stdin.lines().for_each(|line| {
    let line = line.expect("read line");
    // For each wanted entry append it to the want list
    eprintln!("[upload-index] WANTED {:?}", line);
    want_list.insert(PathBuf::from(line));
  });

  // Iterate the tar_path archive and accumulate the wanted entries
  let tar_file = File::open(tar_path).expect("tar file present");
  let mut tar_archive = tar::Archive::new(tar_file);

  let want_output = Vec::new();
  let mut want_builder = tar::Builder::new(want_output);

  eprintln!("[upload-index] generating wanted tarball");
  for file in tar_archive.entries().expect("entries") {
    let mut file = file.expect("entry file");

    {
      let file_path = file.path().expect("entry path");
      if !want_list.contains(&file_path.to_path_buf()) {
        continue;
      }
    }

    let mut new_header = file.header().clone();
    let file_path = file.path().expect("entry path").into_owned();

    want_builder.append_data(&mut new_header, file_path, file).expect("append entry to wanted");
  }

  let want_output = &want_builder.into_inner().expect("finish wanted archive");

  eprintln!("[upload-index] sending wanted tarball");
  tarball_codec::write_tarball("[upload-index]", &mut want_output.as_slice(), &mut stdout).expect("write wanted to receive-index");
  unsafe {
    libc::close(1);
  }

  Ok(())
}