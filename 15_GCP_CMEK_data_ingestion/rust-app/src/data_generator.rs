use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use rand::{prelude::*, thread_rng};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Customer {
    pub customer_id: u32,
    pub first_name: String,
    pub last_name: String,
    pub email: String,
    pub country: String,
    pub registration_date: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Transaction {
    pub transaction_id: String,
    pub customer_id: u32,
    pub amount: f64,
    pub transaction_date: DateTime<Utc>,
    pub currency: String,
    pub description: String,
}

pub struct DataGenerator {
    rng: ThreadRng,
    first_names: Vec<&'static str>,
    last_names: Vec<&'static str>,
    countries: Vec<&'static str>,
    descriptions: Vec<&'static str>,
}

impl DataGenerator {
    pub fn new() -> Self {
        Self {
            rng: thread_rng(),
            first_names: vec![
                "John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eva", "Frank",
                "Grace", "Henry", "Ivy", "Jack", "Kate", "Leo", "Mia", "Noah",
            ],
            last_names: vec![
                "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
                "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson",
            ],
            countries: vec![
                "US", "CA", "UK", "DE", "FR", "ES", "IT", "AU", "JP", "BR", "IN", "CN",
            ],
            descriptions: vec![
                "Online Purchase", "Grocery Store", "Gas Station", "Restaurant",
                "Coffee Shop", "Bookstore", "Electronics", "Clothing", "Pharmacy",
                "Utility Payment", "Insurance", "Subscription",
            ],
        }
    }

    pub async fn generate_customers(&mut self, count: usize) -> Result<Vec<Customer>> {
        let mut customers = Vec::with_capacity(count);
        let base_date = Utc::now() - Duration::days(365);

        for i in 0..count {
            let customer = Customer {
                customer_id: (i + 1) as u32,
                first_name: self.first_names.choose(&mut self.rng).unwrap().to_string(),
                last_name: self.last_names.choose(&mut self.rng).unwrap().to_string(),
                email: format!(
                    "{}{}@example.com",
                    self.first_names.choose(&mut self.rng).unwrap().to_lowercase(),
                    self.rng.gen_range(100..9999)
                ),
                country: self.countries.choose(&mut self.rng).unwrap().to_string(),
                registration_date: base_date + Duration::days(self.rng.gen_range(0..365)),
            };
            customers.push(customer);
        }

        Ok(customers)
    }

    pub async fn generate_transactions(&mut self, count: usize, customer_count: usize) -> Result<Vec<Transaction>> {
        let mut transactions = Vec::with_capacity(count);
        let base_date = Utc::now() - Duration::days(30);

        for i in 0..count {
            let amount_raw: f64 = self.rng.gen_range(10.0..2000.0) * 100.0;
            let amount = amount_raw.round() / 100.0; // Round to 2 decimals
            
            let transaction = Transaction {
                transaction_id: format!("txn_{:08}", i + 1),
                customer_id: self.rng.gen_range(1..=customer_count as u32),
                amount,
                transaction_date: base_date + Duration::days(self.rng.gen_range(0..30)),
                currency: "USD".to_string(),
                description: self.descriptions.choose(&mut self.rng).unwrap().to_string(),
            };
            transactions.push(transaction);
        }

        Ok(transactions)
    }
}

impl Default for DataGenerator {
    fn default() -> Self {
        Self::new()
    }
} 