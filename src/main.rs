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

fn upload_index(matches: &clap::ArgMatches) -> MatchResult {
  let tar_path = matches.value_of("file").unwrap();
  let index_path = matches.value_of("index").unwrap();

  // Send the index

  let mut index_file = File::open(index_path).unwrap();
  let mut index = Vec::new();
  index_file.read_to_end(&mut index).unwrap();

  let mut stdout = io::stdout();
  stdout.write_fmt(format_args!("{}\0", index.len())).unwrap();
  stdout.write(&index).unwrap();

  // Wait to read requested parts on stdin
  let stdin = BufReader::new(io::stdin());
  stdin.lines().for_each(|line| {
    eprintln!("WANTED {:?}", line)

    // For each wanted entry append it to the want list
  });

  // Iterate the tar_path archive and accumulate the wanted entries

  // Write another length prefixed tarball to stdout and then close stdout
  unsafe {
    libc::close(1);
  }

  Ok(())
}

fn receive_index(matches: &clap::ArgMatches) -> MatchResult {
  let destination_path = matches.value_of("destination").unwrap();
  let file = matches.value_of("file").unwrap();

  // Read the index

  let mut stdin = BufReader::new(io::stdin());
  let mut stdout = io::stdout();

  let mut size_buffer = Vec::new();
  stdin.read_until(b'\0', &mut size_buffer).expect("read length");
  let ascii = &size_buffer[0..size_buffer.len()-1];

  let index_length = str::from_utf8(ascii).unwrap().parse::<usize>().unwrap();
  let mut index = vec![0u8; index_length];
  stdin.read_exact(index.as_mut_slice()).expect("read index");

  let decoder = gzip::Decoder::new(index.as_slice()).expect("gzip decoder");
  let mut index_archive = Archive::new(decoder);

  let mut file_path = PathBuf::from(destination_path);
  file_path.push(file);
  let file = File::create(file_path).unwrap();
  let mut builder = Builder::new(file);

  for file in index_archive.entries().expect("entries") {
    // Make sure there wasn't an I/O error
    let mut file = file.unwrap();

    let mut file_hash = Vec::new();
    file.read_to_end(&mut file_hash).unwrap();

    let mut new_header = file.header().clone();

    if file.header().entry_type() == tar::EntryType::Regular {
      // Append to want list
      eprintln!("WANT {:?} {:?} {:?} {:x?}", file.header().entry_type(), file.path(), file.header().size(), file_hash);

      stdout.write_fmt(format_args!("{}\n", file.path().unwrap().to_str().unwrap())).unwrap();
    } else {
      builder.append(&new_header, file).unwrap();
    }
  }

  stdout.flush().unwrap();
  unsafe {
    libc::close(1);
  }

  // Read another length prefixed tarball on stdin

  // Append the entries from the new tarball

  let mut file = builder.into_inner().unwrap();
  file.flush().unwrap();

  Ok(())
}
