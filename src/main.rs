extern crate clap;
use clap::{Arg, App, AppSettings, SubCommand};

extern crate tar;
use tar::{Archive, Builder};

use std::{fs::File, io, io::{Read, Write}};

extern crate sha1;

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
          .help("Tarball file to generate an index of")
          .takes_value(true)
          .required(true)
        )
        .arg(Arg::with_name("output")
          .short("o")
          .long("output")
          .value_name("OUTPUT")
          .help("Output path to write the index to")
          .takes_value(true)
          .required(true)
        )
      )
      .subcommand(SubCommand::with_name("verify")
        .about("Verify an index file for a tarball")
        .arg(Arg::with_name("file")
          .short("f")
          .long("file")
          .value_name("FILE")
          .help("Tarball file to generate an index of")
          .takes_value(true)
          .required(true)
        )
        .arg(Arg::with_name("index")
          .short("i")
          .long("index")
          .value_name("INDEX")
          .help("Path to the index to use for verification")
          .takes_value(true)
          .required(true)
        )
      )
    )
    .get_matches();

  let result = if let Some(matches) = matches.subcommand_matches("index") {
    index(matches)
  } else {
    Err(())
  };

  match result {
    Err(()) => println!("Unknown command"),
    _ => {}
  }
}

type MatchResult = Result<(), ()>;

fn index(matches: &clap::ArgMatches) -> MatchResult {
  if let Some(matches) = matches.subcommand_matches("create") {
    create_index(matches)
  } else if let Some(matches) = matches.subcommand_matches("verify") {
    verify_index(matches)
  } else {
    Err(())
  }
}

fn generate_index(tar_path: &str) -> io::Result<Vec<u8>> {
  let buffer = Vec::new();
  let mut builder = Builder::new(buffer);

  let file = File::open(tar_path).unwrap();
  let mut archive = Archive::new(file);

  for file in archive.entries().unwrap() {
    // Make sure there wasn't an I/O error
    let mut file = file.unwrap();

    let mut file_contents = Vec::new();
    file.read_to_end(&mut file_contents).unwrap();
    let file_sha1 = sha1::Sha1::from(&file_contents).digest().bytes();

    builder.append(file.header(), file_sha1.as_ref()).unwrap();
  }

  builder.into_inner()
}

fn create_index(matches: &clap::ArgMatches) -> MatchResult {
  let tar_path = matches.value_of("file").unwrap();
  let index_path = matches.value_of("output").unwrap();

  let mut index_file = File::create(index_path).unwrap();
  index_file.write_all(&generate_index(tar_path).unwrap()).unwrap();

  Ok(())
}

fn verify_index(matches: &clap::ArgMatches) -> MatchResult {
  Err(())
}
