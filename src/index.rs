use std::{
  fs::File,
  io,
};

extern crate libflate;
use self::libflate::gzip;

extern crate tar;

extern crate sha1;
use self::sha1::{Sha1, Digest};

pub fn create(tar_path: &str) -> io::Result<Vec<u8>> {
  let buffer = Vec::new();
  let encoder = gzip::Encoder::new(buffer)?;
  let mut builder = tar::Builder::new(encoder);

  let file = File::open(tar_path)?;
  let mut archive = tar::Archive::new(file);

  for file in archive.entries()? {
    // Make sure there wasn't an I/O error
    let mut file = file?;

    let file_path = file.path()?.into_owned();
    let mut new_header = file.header().clone();

    if file.header().entry_type() == tar::EntryType::Regular {
      let file_hash = Sha1::digest_reader(&mut file)?;

      new_header.set_size(file_hash.len() as u64);
      new_header.set_cksum();

      builder.append_data(&mut new_header, file_path, file_hash.as_ref())?;
    } else {
      builder.append_data(&mut new_header, file_path, file)?;
    }
  }

  builder.into_inner()?.finish().into_result()
}