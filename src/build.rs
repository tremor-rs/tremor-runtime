// Copyright 2020-2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


fn get_git_branch() -> String {
  use std::process::Command;

  let branch = Command::new("git")
      .arg("rev-parse")
      .arg("--abbrev-ref")
      .arg("HEAD")
      .output();
  if let Ok(branch_output) = branch {
      let branch_string = String::from_utf8_lossy(&branch_output.stdout);
      return branch_string.lines().next().unwrap_or("unknown_branch").to_string();
  } else {
      panic!("Can not get git branch: {}", branch.unwrap_err());
  }
}

fn get_git_commit() -> String {
  use std::process::Command;

  let commit = Command::new("git")
      .arg("rev-parse")
      .arg("--verify")
      .arg("HEAD")
      .output();

  if let Ok(commit_output) = commit {
      let commit_string = String::from_utf8_lossy(&commit_output.stdout);
      return commit_string.lines().next().unwrap_or("unknown_hash").to_string();
  } else {
      panic!("Can not get git commit: {}", commit.unwrap_err());
  }
}

fn main() {
  println!(
    "cargo:rustc-env=VERSION_BRANCH={}", 
    get_git_branch()
  );
  println!(
    "cargo:rustc-env=VERSION_HASH={}", 
    get_git_commit()
  );
}