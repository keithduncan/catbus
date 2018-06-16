use std::{str, io::{self, Read, Write, BufRead, BufReader}};

pub fn write_tarball<R: ?Sized, W: ?Sized>(name: &str, r: &mut R, w: &mut W) -> io::Result<()>
  where R: Read, W: Write {
  let mut tarball = Vec::new();
  r.read_to_end(&mut tarball)?;
  eprintln!("{} write tarball {}", name, tarball.len());
  w.write_fmt(format_args!("{}\0", tarball.len()))?;
  w.write(&tarball)?;
  w.flush()
}

pub fn read_tarball<T: Read>(name: &str, r: &mut BufReader<T>) -> io::Result<Vec<u8>> {
  let mut size_buffer = Vec::new();
  r.read_until(b'\0', &mut size_buffer)?;
  let ascii = &size_buffer[0..size_buffer.len()-1];
  let tarball_length = str::from_utf8(ascii).expect("length prefix is uft8").parse::<usize>().expect("parse length prefix");

  eprintln!("{} read tarball {}", name, tarball_length);

  let mut tarball = vec![0u8; tarball_length];
  r.read_exact(tarball.as_mut_slice())?;

  Ok(tarball)
}