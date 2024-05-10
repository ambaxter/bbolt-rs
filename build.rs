use cfg_aliases::cfg_aliases;

fn main() {
  cfg_aliases! {
    timeout_supported: {any(target_os = "linux", target_os = "macos", target_os = "darwin")},
    mlock_supported: {target_family = "unix"},
    mmap_advise_supported: {target_family = "unix"},
  }
}
