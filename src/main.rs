extern crate libc;

extern crate clap;
use clap::{Arg, App, AppSettings, SubCommand};

extern crate tar;

use std::{
  fs::File,
  io::{self, Write},
};

extern crate digest;

extern crate catbus;
use catbus::{index, upload, receive};

#[derive(Debug)]
enum MainError {
  IndexError(IndexError),
  TransportError(TransportError),
  UnknownCommand,
}

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
      .map_err(MainError::IndexError)
  } else if let Some(matches) = matches.subcommand_matches("transport") {
    transport(matches)
      .map_err(MainError::TransportError)
  } else {
    Err(MainError::UnknownCommand)
  };

  match result {
    Err(e) => panic!("{:#?}", e),
    _ => {}
  }
}

#[derive(Debug)]
enum IndexError {
  CreateIndex(io::Error),
  UnknownCommand,
}

fn index(matches: &clap::ArgMatches) -> Result<(), IndexError> {
  if let Some(matches) = matches.subcommand_matches("create") {
    create_index(matches)
      .map_err(IndexError::CreateIndex)
  } else {
    Err(IndexError::UnknownCommand)
  }
}

fn create_index(matches: &clap::ArgMatches) -> io::Result<()> {
  let tar_path = matches.value_of("file").expect("file arg required");
  let index_path = matches.value_of("output").expect("output arg required");

  let mut index_file = File::create(index_path)?;
  index_file.write_all(&index::create(tar_path)?)?;

  Ok(())
}

#[derive(Debug)]
enum TransportError {
  UploadIndex(upload::UploadIndexError),
  ReceiveIndex(receive::ReceiveIndexError),
  UnknownCommand,
}

fn transport(matches: &clap::ArgMatches) -> Result<(), TransportError> {
  if let Some(matches) = matches.subcommand_matches("upload-index") {
    let tar_path = matches.value_of("file").expect("file arg required");
    let index_path = matches.value_of("index").expect("index arg required");

    upload::upload_index(tar_path, index_path)
      .map_err(TransportError::UploadIndex)
  } else if let Some(matches) = matches.subcommand_matches("receive-index") {
    let destination_path = matches.value_of("destination").expect("destination arg required");
    let destination_file = matches.value_of("file").expect("file arg required");

    receive::receive_index(destination_path.as_ref(), destination_file)
      .map_err(TransportError::ReceiveIndex)
  } else {
    Err(TransportError::UnknownCommand)
  }
}
