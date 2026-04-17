fn main() {
    // Embed git commit SHA at compile time (ADR 070).
    //
    // Fallback chain:
    // 1. VLINDER_BUILD_SHA env var (set in CI/packaging scripts)
    // 2. git rev-parse HEAD + "-dirty" suffix if working tree is dirty
    // 3. "unknown" fallback (e.g., crates.io install with no .git)
    let sha = std::env::var("VLINDER_BUILD_SHA").unwrap_or_else(|_| git_sha());
    println!("cargo:rustc-env=VLINDER_GIT_SHA={sha}");
}

fn git_sha() -> String {
    let head = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output();

    let Ok(output) = head else {
        return "unknown".to_string();
    };

    if !output.status.success() {
        return "unknown".to_string();
    }

    let mut sha = String::from_utf8_lossy(&output.stdout).trim().to_string();

    // Check for dirty working tree
    let dirty = std::process::Command::new("git")
        .args(["diff", "--quiet"])
        .status()
        .is_ok_and(|s| !s.success());

    if dirty {
        sha.push_str("-dirty");
    }

    sha
}
