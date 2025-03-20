use plotters::prelude::*;
use rand::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a 800x600 bitmap to draw on
    let root = BitMapBackend::new("scatter_chart.png", (800, 600)).into_drawing_area();
    root.fill(&WHITE)?;

    // Define the chart area 
    let mut chart = ChartBuilder::on(&root)
        .caption("Data Engineering: Creativity vs. Difficulty", ("sans-serif", 30).into_font())
        .margin(10)
        .x_label_area_size(40)
        .y_label_area_size(40)
        .build_cartesian_2d(0.0..10.0, 0.0..10.0)?;

    // Configure the chart
    chart
        .configure_mesh()
        .x_labels(10)
        .y_labels(10)
        .x_desc("Difficulty Level")
        .y_desc("Creativity Level")
        .axis_desc_style(("sans-serif", 15))
        .disable_mesh()
        .draw()?;

    // Define data points
    // Format: (Difficulty, Creativity, Label)
    let data_points = vec![
        (10.0, 10.0, "Architecture Design"),
        (9.0, 9.0, "Edge Case Solving"),
        (8.0, 8.0, "Data Modeling"),
        (7.0, 7.0, "Pipeline Tuning"),
        (6.0, 6.0, "Quality Framework"),
        (5.0, 5.0, "Custom ETL Logic"),
        (4.0, 4.0, "Complex ETL"),
        (3.5, 3.5, "ETL Improvement"),
        (3.0, 3.0, "System Docs"),
        (2.5, 2.5, "Config Mgmt"),
        (2.0, 2.0, "Compliance"),
        (1.5, 1.5, "Basic ETL"),
        (1.0, 1.0, "Monitoring"),
        (0.5, 0.5, "Maintenance"),
        (0.25, 0.25, "Basic Docs"),
    ];

    // Helper function to get color based on value
    let get_gradient_color = |value: f64| -> RGBColor {
        // Normalize value to 0-1 range (assuming max is 20.0 for difficulty + creativity)
        let normalized = value / 20.0;
        // Create a gradient from green to red
        let r = (normalized * 255.0) as u8;
        let g = ((1.0 - normalized) * 255.0) as u8;
        let b = 0;
        RGBColor(r, g, b)
    };

    // Draw data points
    chart.draw_series(data_points.iter().map(|point| {
        // Add a small random shift to points with the same coordinates
        // to avoid complete overlap
        let mut rng = rand::thread_rng();
        let x_jitter = if point.0 < 9.5 { rng.gen_range(-0.1..0.1) } else { 0.0 };
        let y_jitter = if point.1 < 9.5 { rng.gen_range(-0.1..0.1) } else { 0.0 };
        
        // Calculate combined value and get gradient color
        let combined_value = point.0 + point.1;
        let color = get_gradient_color(combined_value);
        
        return Circle::new(
            (point.0 + x_jitter, point.1 + y_jitter),
            5, 
            &color
        );
    }))?;

    // Add labels for key points
    for (idx, point) in data_points.iter().enumerate() {
        if point.1 >= 7.0 || idx % 3 == 0 || idx >= data_points.len() - 2 {  // Label high creativity, every 3rd point, and the last two points
            // Special handling for different points based on position
            let (font_size, x_offset, y_offset) = if idx >= data_points.len() - 2 {
                // Bottom points - place labels above points
                (18, 0.0, 0.5)  // Increased from 10 to 15
            } else if idx < 2 {
                // Top points - place labels below and left of points to keep them in view
                (18, -0.5, -0.3)  // Increased from 12 to 18
            } else {
                (18, 0.2, 0.2)  // Increased from 12 to 18
            };
            
            chart.draw_series(std::iter::once(Text::new(
                point.2.to_string(),
                (point.0 + x_offset, point.1 + y_offset),
                ("sans-serif", font_size).into_font(),
            )))?;
        }
    }

    // Add trend line
    chart.draw_series(LineSeries::new(
        vec![(1.0, 1.0), (10.0, 10.0)],
        &RED.mix(0.5),
    ))?;

    // Add key areas with matching colors
    chart.draw_series(std::iter::once(Text::new(
        "High Creativity & High Difficulty",
        (6.5, 8.5),
        ("sans-serif", 15).into_font().color(&get_gradient_color(17.0)),
    )))?;
    
    chart.draw_series(std::iter::once(Text::new(
        "Moderate Creativity & Difficulty",
        (5.5, 5.0),
        ("sans-serif", 15).into_font().color(&get_gradient_color(10.0)),
    )))?;
    
    chart.draw_series(std::iter::once(Text::new(
        "Low Creativity & Difficulty",
        (2.5, 2.0),
        ("sans-serif", 15).into_font().color(&get_gradient_color(3.0)),
    )))?;

    // To avoid the IO error being ignored silently, we manually call the present function
    root.present()?;
    
    println!("Scatter chart has been saved as 'scatter_chart.png'");
    Ok(())
}
