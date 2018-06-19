use std::{
  path::PathBuf,
  io::{
    self,
    BufRead,
    BufReader,
  },
  fs::File,
  collections::HashSet,
};

use tarball_codec;

extern crate tar;

extern crate libc;

#[derive(Debug)]
pub enum UploadIndexError {
  Io(io::Error),
}

impl From<io::Error> for UploadIndexError {
  fn from(e: io::Error) -> Self {
    UploadIndexError::Io(e)
  }
}

pub fn upload_index(tar_path: &str, index_path: &str) -> Result<(), UploadIndexError> {
  // Output on stdout
  let mut stdout = io::stdout();

  // Send the index first
  eprintln!("[upload-index] sending index tarball");
  let mut index_file = File::open(index_path)?;
  tarball_codec::write("[upload-index]", &mut index_file, &mut stdout)?;

  let mut want_list = HashSet::new();

  // Wait to read requested parts on stdin
  eprintln!("[upload-index] reading want lines");
  let stdin = BufReader::new(io::stdin());

  stdin
    .lines()
    .map(|line| {
      let line = line?;

      // For each wanted entry append it to the want list
      eprintln!("[upload-index] WANTED {:?}", line);
      want_list.insert(PathBuf::from(line));

      Ok(())
    })
    .collect::<io::Result<Vec<()>>>()?;

  // Iterate the tar_path archive and accumulate the wanted entries
  let tar_file = BufReader::new(File::open(tar_path)?);
  let mut tar_archive = tar::Archive::new(tar_file);

  let mut want_builder = tar::Builder::new(Vec::new());

  eprintln!("[upload-index] generating wanted tarball");
  for file in tar_archive.entries()? {
    let mut file = file?;

    // TODO optimise out allocation?
    let file_path = file.path()?.to_path_buf();
    if !want_list.contains(&file_path) {
      continue;
    }

    let mut new_header = file.header().clone();

    want_builder.append_data(&mut new_header, file_path, file)?;
  }

  let want_output = &want_builder.into_inner()?;

  eprintln!("[upload-index] sending wanted tarball");
  tarball_codec::write("[upload-index]", &mut want_output.as_slice(), &mut stdout)?;
  unsafe {
    libc::close(1);
  }

  Ok(())
}