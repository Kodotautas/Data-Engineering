use fake::faker::lorem::en::Sentence;
use fake::faker::company::en::Bs;
use fake::faker::internet::en::{IPv4, SafeEmail};
use fake::faker::name::en::Name;
use fake::faker::phone_number::en::PhoneNumber;
use fake::{Fake};
use uuid::Uuid;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use chrono::{Datelike, NaiveDate, Utc};
use rand::Rng;

use arrow::array::{BooleanArray, StringArray, UInt8Array, Date32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;

#[derive(Debug)]
struct UserRecord {
    uuid: String,
    full_name: String,
    email: String,
    phone_number: String,
    address: String,
    date_of_birth: NaiveDate,
    ip_address: String,
    sensitive_notes: String,
    user_preferences: String,
    account_status: String,
    last_login_date: NaiveDate,
    signup_date: NaiveDate,
    profile_completion_score: u8,
    preferred_language: String,
    marketing_consent: bool,
}

fn generate_fake_user_records(count: usize) -> Vec<UserRecord> {
    let mut records = Vec::new();
    let mut rng = rand::thread_rng();
    let today = Utc::now().date_naive();

    let account_statuses = ["active", "inactive", "suspended", "pending_verification"];
    let languages = ["en", "es", "fr", "de", "ja", "pt"];

    for _ in 0..count {
        let birth_year = rng.gen_range((today.year() - 80)..=(today.year() - 18));
        let birth_month = rng.gen_range(1..=12);
        let birth_day = rng.gen_range(1..=28);
        let date_of_birth_dt = NaiveDate::from_ymd_opt(birth_year, birth_month, birth_day)
            .unwrap_or(today);

        let days_since_last_login = rng.gen_range(0..=365);
        let last_login_dt = today - chrono::Duration::days(days_since_last_login);

        let min_signup_date = date_of_birth_dt + chrono::Duration::days(13 * 365);
        let max_days_between_signup_and_last_login = (last_login_dt - min_signup_date).num_days();
        let signup_date_dt = if max_days_between_signup_and_last_login > 0 {
            last_login_dt - chrono::Duration::days(rng.gen_range(0..=max_days_between_signup_and_last_login))
        } else {
            last_login_dt
        };
        
        let status_index = rng.gen_range(0..account_statuses.len());
        let account_status_str = account_statuses[status_index].to_string();

        let user_preferences_str = format!("{{\"theme\": \"{}\", \"notifications\": {}}}", 
                                        if rng.gen_bool(0.5) {"dark"} else {"light"}, 
                                        rng.gen_bool(0.7));
        
        let profile_completion_score_val = rng.gen_range(20..=100);

        let lang_index = rng.gen_range(0..languages.len());
        let preferred_language_str = languages[lang_index].to_string();

        let marketing_consent_val = rng.gen_bool(0.6);

        let record = UserRecord {
            uuid: Uuid::new_v4().to_string(),
            full_name: Name().fake(),
            email: SafeEmail().fake(),
            phone_number: PhoneNumber().fake(),
            address: Sentence(3..5).fake(), 
            date_of_birth: date_of_birth_dt,
            ip_address: IPv4().fake(),
            sensitive_notes: Bs().fake(),
            user_preferences: user_preferences_str,
            account_status: account_status_str,
            last_login_date: last_login_dt,
            signup_date: signup_date_dt,
            profile_completion_score: profile_completion_score_val,
            preferred_language: preferred_language_str,
            marketing_consent: marketing_consent_val,
        };
        records.push(record);
    }
    records
}

pub fn create_fake_data_parquet(file_path_str: &str, num_records: usize) -> Result<(), Box<dyn Error>> {
    let output_dir = Path::new(file_path_str).parent().unwrap_or_else(|| Path::new("."));
    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir)?;
    }

    let records = generate_fake_user_records(num_records);

    let schema = Arc::new(Schema::new(vec![
        Field::new("uuid", DataType::Utf8, false),
        Field::new("full_name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("phone_number", DataType::Utf8, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("date_of_birth", DataType::Date32, false),
        Field::new("ip_address", DataType::Utf8, false),
        Field::new("sensitive_notes", DataType::Utf8, false),
        Field::new("user_preferences", DataType::Utf8, false),
        Field::new("account_status", DataType::Utf8, false),
        Field::new("last_login_date", DataType::Date32, false),
        Field::new("signup_date", DataType::Date32, false),
        Field::new("profile_completion_score", DataType::UInt8, false),
        Field::new("preferred_language", DataType::Utf8, false),
        Field::new("marketing_consent", DataType::Boolean, false),
    ]));
    
    let epoch_date: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).expect("Epoch date 1970-01-01 must be valid");

    let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
    columns.push(Arc::new(StringArray::from_iter_values(records.iter().map(|r| r.uuid.as_str()))));
    columns.push(Arc::new(StringArray::from_iter_values(records.iter().map(|r| r.full_name.as_str()))));
    columns.push(Arc::new(StringArray::from_iter_values(records.iter().map(|r| r.email.as_str()))));
    columns.push(Arc::new(StringArray::from_iter_values(records.iter().map(|r| r.phone_number.as_str()))));
    columns.push(Arc::new(StringArray::from_iter_values(records.iter().map(|r| r.address.as_str()))));
    columns.push(Arc::new(Date32Array::from_iter_values(records.iter().map(|r| r.date_of_birth.signed_duration_since(epoch_date).num_days() as i32))));
    columns.push(Arc::new(StringArray::from_iter_values(records.iter().map(|r| r.ip_address.as_str()))));
    columns.push(Arc::new(StringArray::from_iter_values(records.iter().map(|r| r.sensitive_notes.as_str()))));
    columns.push(Arc::new(StringArray::from_iter_values(records.iter().map(|r| r.user_preferences.as_str()))));
    columns.push(Arc::new(StringArray::from_iter_values(records.iter().map(|r| r.account_status.as_str()))));
    columns.push(Arc::new(Date32Array::from_iter_values(records.iter().map(|r| r.last_login_date.signed_duration_since(epoch_date).num_days() as i32))));
    columns.push(Arc::new(Date32Array::from_iter_values(records.iter().map(|r| r.signup_date.signed_duration_since(epoch_date).num_days() as i32))));
    columns.push(Arc::new(UInt8Array::from_iter_values(records.iter().map(|r| r.profile_completion_score))));
    columns.push(Arc::new(StringArray::from_iter_values(records.iter().map(|r| r.preferred_language.as_str()))));
    columns.push(Arc::new(BooleanArray::from_iter(records.iter().map(|r| Some(r.marketing_consent)))));

    let batch = RecordBatch::try_new(schema.clone(), columns)?;

    let file = File::create(file_path_str)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

    writer.write(&batch)?;
    writer.close()?;

    println!("Successfully generated {} fake records to {}", num_records, file_path_str);
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let output_file = "data/sensitive_data.parquet";
    let num_records_to_generate = 1000000000;

    println!("Attempting to generate fake data (Parquet)...");
    create_fake_data_parquet(output_file, num_records_to_generate)?;
    Ok(())
} 