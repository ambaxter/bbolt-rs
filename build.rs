use cfg_aliases::cfg_aliases;

fn main() {
  cfg_aliases! {
    use_timeout: {any(target_os = "linux", target_os = "macos", target_os = "darwin")},
    use_mlock: {target_family = "unix"}
  }
}
