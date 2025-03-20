# Data Engineering Creativity vs. Difficulty Scatter Chart

This Rust project creates a scatter chart visualizing the relationship between creativity and difficulty levels in various data engineering tasks.

## About the Chart

The chart plots data engineering tasks on two axes:
- **X-axis**: Difficulty Level (1-10)
- **Y-axis**: Creativity Level (1-10)

The chart visually demonstrates the correlation between task difficulty and required creativity in data engineering work, showing that more complex tasks typically require higher levels of creativity.

## Requirements

- Rust (installed via [rustup](https://rustup.rs/))
- Cargo (comes with Rust)

## Installation

1. Clone this repository:
   ```bash
   git clone <repository-url>
   ```

2. Navigate to the project directory:
   ```bash
   cd scatter_chart_example
   ```

## Usage

1. Build and run the project:
   ```bash
   cargo run
   ```

2. The output image `scatter_chart.png` will be generated in the project root directory.

## Libraries Used

- **plotters**: A powerful Rust drawing library used for creating the scatter chart
- **rand**: Used to add small random shifts to overlapping data points

## Data Source

The data points used in this chart are derived from an analysis of various data engineering tasks categorized by creativity and difficulty levels.