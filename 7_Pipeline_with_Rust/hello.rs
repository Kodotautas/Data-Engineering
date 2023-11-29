use std::io;

fn main() {
    println!("What is your name?");

    let mut name = String::new();

    io::stdin().read_line(&mut name)
        .expect("Failed to read input.");

    println!("Hello, {}! Welcome to the world of Rust!", name.trim());
}