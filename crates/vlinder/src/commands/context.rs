use std::path::PathBuf;

use clap::Subcommand;

use crate::config::vlinder_dir;

#[derive(Subcommand, Debug, PartialEq)]
pub enum ContextCommand {
    /// Create a new context from the current client.toml
    New { name: String },
    /// List available contexts
    List,
    /// Show the active context
    Get,
    /// Switch to a context
    Use { name: String },
    /// Delete a context
    Delete { name: String },
}

#[allow(clippy::unused_async)]
pub async fn execute(cmd: ContextCommand) {
    match cmd {
        ContextCommand::New { name } => new_context(&name),
        ContextCommand::List => list_contexts(),
        ContextCommand::Get => get_context(),
        ContextCommand::Use { name } => use_context(&name),
        ContextCommand::Delete { name } => delete_context(&name),
    }
}

fn context_path(name: &str) -> PathBuf {
    vlinder_dir().join(format!("client.{name}.toml"))
}

fn client_toml() -> PathBuf {
    vlinder_dir().join("client.toml")
}

fn prompt(label: &str, default: &str) -> String {
    eprint!("{label} [{default}]: ");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap_or_default();
    let trimmed = input.trim();
    if trimmed.is_empty() {
        default.to_string()
    } else {
        trimmed.to_string()
    }
}

fn new_context(name: &str) {
    let target = context_path(name);

    if target.exists() {
        eprintln!("Context '{name}' already exists");
        std::process::exit(1);
    }

    let defaults = crate::config::DaemonConfig::default();

    eprintln!("Creating context '{name}'. Press Enter to accept defaults.\n");

    let registry_addr = prompt("Registry address", &defaults.registry_addr);
    let harness_addr = prompt("Harness address", &defaults.harness_addr);
    let state_addr = prompt("State address", &defaults.state_addr);
    let secret_addr = prompt("Secret address", &defaults.secret_addr);
    let catalog_addr = prompt("Catalog address", &defaults.catalog_addr);

    let contents = format!(
        "[daemon]\nregistry_addr = \"{registry_addr}\"\nharness_addr = \"{harness_addr}\"\nstate_addr = \"{state_addr}\"\nsecret_addr = \"{secret_addr}\"\ncatalog_addr = \"{catalog_addr}\"\n"
    );

    std::fs::write(&target, contents).unwrap_or_else(|e| {
        eprintln!("Failed to write {}: {e}", target.display());
        std::process::exit(1);
    });

    println!("Created context '{name}'");
}

fn list_contexts() {
    let dir = vlinder_dir();
    let active = active_context_name();

    let mut found = false;
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(ctx) = name
                .strip_prefix("client.")
                .and_then(|s| s.strip_suffix(".toml"))
            {
                let marker = if active.as_deref() == Some(ctx) {
                    " *"
                } else {
                    ""
                };
                println!("{ctx}{marker}");
                found = true;
            }
        }
    }

    if !found {
        println!("No contexts found. Create one with: vlinder context new <name>");
    }
}

fn get_context() {
    match active_context_name() {
        Some(name) => println!("{name}"),
        None => {
            println!("No active context (using default client.toml)");
        }
    }
}

fn use_context(name: &str) {
    let target = context_path(name);
    if !target.exists() {
        eprintln!("Context '{name}' does not exist");
        eprintln!("Available contexts:");
        list_contexts();
        std::process::exit(1);
    }

    let link = client_toml();

    // Remove existing client.toml (file or symlink)
    if link.exists() || link.symlink_metadata().is_ok() {
        std::fs::remove_file(&link).unwrap_or_else(|e| {
            eprintln!("Failed to remove client.toml: {e}");
            std::process::exit(1);
        });
    }

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&target, &link).unwrap_or_else(|e| {
            eprintln!("Failed to create symlink: {e}");
            std::process::exit(1);
        });
    }

    #[cfg(not(unix))]
    {
        // Fallback: copy the file
        std::fs::copy(&target, &link).unwrap_or_else(|e| {
            eprintln!("Failed to copy config: {e}");
            std::process::exit(1);
        });
    }

    println!("Switched to context '{name}'");
}

fn delete_context(name: &str) {
    let target = context_path(name);
    if !target.exists() {
        eprintln!("Context '{name}' does not exist");
        std::process::exit(1);
    }

    // If active context is being deleted, remove the symlink too
    if active_context_name().as_deref() == Some(name) {
        let link = client_toml();
        if link.symlink_metadata().is_ok() {
            let _ = std::fs::remove_file(&link);
        }
    }

    std::fs::remove_file(&target).unwrap_or_else(|e| {
        eprintln!("Failed to delete context: {e}");
        std::process::exit(1);
    });

    println!("Deleted context '{name}'");
}

/// Resolve which context is active by reading the symlink target.
fn active_context_name() -> Option<String> {
    context_name_from_dir(&vlinder_dir())
}

fn context_name_from_dir(dir: &std::path::Path) -> Option<String> {
    let link = dir.join("client.toml");
    std::fs::read_link(&link).ok().and_then(|target| {
        target
            .file_name()?
            .to_string_lossy()
            .strip_prefix("client.")?
            .strip_suffix(".toml")
            .map(String::from)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_path_format() {
        let path = context_path("aws");
        assert!(path.to_string_lossy().ends_with("client.aws.toml"));
    }

    #[test]
    fn active_context_returns_none_when_not_symlink() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("client.toml"), "").unwrap();

        assert!(super::context_name_from_dir(dir.path()).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn active_context_resolves_symlink() {
        let dir = tempfile::tempdir().unwrap();

        let ctx_file = dir.path().join("client.local.toml");
        std::fs::write(&ctx_file, "").unwrap();

        let link = dir.path().join("client.toml");
        std::os::unix::fs::symlink(&ctx_file, &link).unwrap();

        assert_eq!(
            super::context_name_from_dir(dir.path()),
            Some("local".to_string())
        );
    }
}
