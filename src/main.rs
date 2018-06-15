extern crate libc;

extern crate clap;
use clap::{Arg, App, AppSettings, SubCommand};

extern crate tar;
use tar::{Archive, Builder};

extern crate libflate;
use libflate::gzip;

use std::{path::PathBuf, fs::File, str, io, io::{Read, Write, BufRead, BufReader}};

extern crate sha1;
use sha1::{Sha1, Digest};

extern crate digest;

fn main() {
  let matches = App::new("catbus")
    .version("1.0")
    .author("Keith Duncan <keith_duncan@me.com>")
    .about("Stream tarballs over the network efficiently")
    .setting(AppSettings::SubcommandRequired)
    .subcommand(SubCommand::with_name("index")
      .about("controls tarball indexes")
      .setting(AppSettings::SubcommandRequired)
      .subcommand(SubCommand::with_name("create")
        .about("Create an index file for a tarball")
        .arg(Arg::with_name("file")
          .short("f")
          .long("file")
          .value_name("FILE")
          .help("Path to the tarball file to generate an index of")
          .takes_value(true)
          .required(true)
        )
        .arg(Arg::with_name("output")
          .short("o")
          .long("output")
          .value_name("OUTPUT")
          .help("Output path to write the index file to")
          .takes_value(true)
          .required(true)
        )
      )
    )
    .subcommand(SubCommand::with_name("transport")
      .setting(AppSettings::SubcommandRequired)
      .subcommand(SubCommand::with_name("upload-index")
        .about("Write an index on stdout, receive part requests on stdin and send those.")
        .arg(Arg::with_name("file")
          .short("f")
          .long("file")
          .value_name("FILE")
          .help("Path to the tarball file to transport to the remote peer")
          .takes_value(true)
          .required(true)
        )
        .arg(Arg::with_name("index")
          .short("i")
          .long("index")
          .value_name("INDEX")
          .help("Path to the index file to transport to the remote peer")
          .takes_value(true)
          .required(true)
        )
      )
      .subcommand(SubCommand::with_name("receive-index")
        .about("Receive an index on stdin, construct a tarball using a library of parts or request parts over stdout.")
        .arg(Arg::with_name("destination")
          .short("d")
          .long("destination")
          .value_name("DESTINATION")
          .help("Path to the directory of tarball and index files to use for tarball construction, and ultimate tarball write")
          .takes_value(true)
          .required(true)
        )
        .arg(Arg::with_name("file")
          .short("f")
          .long("file")
          .value_name("FILE")
          .help("File name to create in the destination directory")
          .takes_value(true)
          .required(true)
        )
      )
    )
    .get_matches();

  let result = if let Some(matches) = matches.subcommand_matches("index") {
    index(matches)
  } else if let Some(matches) = matches.subcommand_matches("transport") {
    transport(matches)
  } else {
    Err(())
  };

  match result {
    Err(()) => panic!("Unknown command"),
    _ => {}
  }
}

type MatchResult = Result<(), ()>;

fn index(matches: &clap::ArgMatches) -> MatchResult {
  if let Some(matches) = matches.subcommand_matches("create") {
    create_index(matches)
  } else {
    Err(())
  }
}

fn generate_index(tar_path: &str) -> Vec<u8> {
  let buffer = Vec::new();
  let encoder = gzip::Encoder::new(buffer).unwrap();
  let mut builder = Builder::new(encoder);

  let file = File::open(tar_path).unwrap();
  let mut archive = Archive::new(file);

  for file in archive.entries().unwrap() {
    // Make sure there wasn't an I/O error
    let mut file = file.unwrap();

    let mut new_header = file.header().clone();

    if file.header().entry_type() == tar::EntryType::Regular {
      let file_hash = Sha1::digest_reader(&mut file).unwrap();

      new_header.set_size(file_hash.len() as u64);
      new_header.set_cksum();

      builder.append(&new_header, file_hash.as_ref()).unwrap();
    } else {
      builder.append(&new_header, file).unwrap();
    }
  }

  builder.into_inner().unwrap().finish().into_result().unwrap()
}

fn create_index(matches: &clap::ArgMatches) -> MatchResult {
  let tar_path = matches.value_of("file").unwrap();
  let index_path = matches.value_of("output").unwrap();

  let mut index_file = File::create(index_path).unwrap();
  index_file.write_all(&generate_index(tar_path)).unwrap();

  Ok(())
}

fn transport(matches: &clap::ArgMatches) -> MatchResult {
  if let Some(matches) = matches.subcommand_matches("upload-index") {
    upload_index(matches)
  } else if let Some(matches) = matches.subcommand_matches("receive-index") {
    receive_index(matches)
  } else {
    Err(())
  }
}

fn write_tarball<R: ?Sized, W: ?Sized>(r: &mut R, w: &mut W) -> io::Result<()>
  where R: Read, W: Write {
  let mut tarball = Vec::new();
  r.read_to_end(&mut tarball)?;
  w.write_fmt(format_args!("{}\0", tarball.len()))?;
  w.write(&tarball)?;
  w.flush()
}

fn read_tarball<T: Read>(r: &mut BufReader<T>) -> io::Result<Vec<u8>> {
  let mut size_buffer = Vec::new();
  r.read_until(b'\0', &mut size_buffer)?;
  let ascii = &size_buffer[0..size_buffer.len()-1];
  let tarball_length = str::from_utf8(ascii).unwrap().parse::<usize>().unwrap();

  let mut tarball = vec![0u8; tarball_length];
  r.read_exact(tarball.as_mut_slice())?;

  Ok(tarball)
}

fn upload_index(matches: &clap::ArgMatches) -> MatchResult {
  let tar_path = matches.value_of("file").unwrap();
  let index_path = matches.value_of("index").unwrap();

  // Output on stdout
  let mut stdout = io::stdout();

  // Send the index first
  let mut index_file = File::open(index_path).unwrap();
  write_tarball(&mut index_file, &mut stdout).unwrap();

  let mut want_list = Vec::new();

  // Wait to read requested parts on stdin
  let stdin = BufReader::new(io::stdin());
  stdin.lines().for_each(|line| {
    let line = line.unwrap();
    // For each wanted entry append it to the want list
    eprintln!("WANTED {:?}", line);
    want_list.push(line.to_string())
  });

  // Iterate the tar_path archive and accumulate the wanted entries
  let tar_file = File::open(tar_path).unwrap();
  let mut tar_archive = Archive::new(tar_file);

  let want_output = Vec::new();
  let mut want_builder = Builder::new(want_output);

  for file in tar_archive.entries().expect("entries") {
    let mut file = file.unwrap();

    {
      let file_path = file.path().unwrap();
      if want_list.iter().position(|e| e.as_str() == file_path.as_ref().to_string_lossy()) == None {
        continue;
      }
    }

    let mut file_content = Vec::new();
    file.read_to_end(&mut file_content).unwrap();

    let mut new_header = file.header().clone();

    want_builder.append(&new_header, file).unwrap();
  }

  let want_output = &want_builder.into_inner().unwrap();

  write_tarball(&mut want_output.as_slice(), &mut stdout).unwrap();
  unsafe {
    libc::close(1);
  }

  Ok(())
}

fn receive_index(matches: &clap::ArgMatches) -> MatchResult {
  let destination_path = matches.value_of("destination").unwrap();
  let destination_file = matches.value_of("file").unwrap();

  let mut stdin = BufReader::new(io::stdin());
  let mut stdout = io::stdout();

  // Destination we're going to write a full tarball to
  let mut output_path = PathBuf::from(destination_file);
  output_path.push(destination_file);
  let output = File::create(output_path).unwrap();
  let mut output_builder = Builder::new(output);

  // Read the index
  let index = read_tarball(&mut stdin).unwrap();

  // The index is always compressed
  let decoder = gzip::Decoder::new(index.as_slice()).expect("gzip decoder");
  let mut index_archive = Archive::new(decoder);

  for file in index_archive.entries().expect("entries") {
    let mut file = file.unwrap();

    let mut file_hash = Vec::new();
    file.read_to_end(&mut file_hash).unwrap();

    let mut new_header = file.header().clone();

    if file.header().entry_type() == tar::EntryType::Regular {
      // TODO try to find entry from the local library of parts
      // in destination_path

      // Tell sender we want it
      eprintln!("WANT {:?} {:?} {:?} {:x?}", file.header().entry_type(), file.path(), file.header().size(), file_hash);
      stdout.write_fmt(format_args!("{}\n", file.path().unwrap().to_str().unwrap())).unwrap();
    } else {
      output_builder.append(&new_header, file).unwrap();
    }
  }

  // Tell the sender EOF so they send the want parts
  stdout.flush().unwrap();
  unsafe {
    libc::close(1);
  }

  // Read the tarball of wanted parts
  let want = read_tarball(&mut stdin).unwrap();

  // Append it to the archive we've built it
  let mut want_archive = Archive::new(want.as_slice());
  for file in want_archive.entries().expect("entries") {
    let mut file = file.unwrap();

    let mut new_header = file.header().clone();
    output_builder.append(&new_header, file).unwrap();
  }

  let mut output = output_builder.into_inner().unwrap();
  output.flush().unwrap();

  Ok(())
}
