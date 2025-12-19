use chrono::{Duration, NaiveDateTime};
use rand::Rng;
use std::fs::File;
use std::io::{BufWriter, Write};

// use chrono::{Datelike, TimeZone, Utc};
// use cron::Schedule;
// use std::str::FromStr;

// fn main() {
//     // Approach 1: Using cron expression for days 28-31, filter for last day only
//     println!("=== Approach 1: Cron with filtering ===");
//     let expression = "0 0 10 * * 1,4";
//     let schedule = Schedule::from_str(expression).unwrap();
//     println!("Upcoming fire times:");
//     for datetime in schedule.upcoming(Utc).take(10) {
//         println!("-> {}", datetime);
//     }
// }

// fn main() {
//     let string = "{\n  \"eventType\": \"4063bf7b-4ee0-42c1-8030-7fc272cf6b0b\",\n  \"shortDesc\": \"OMS_HR_Consumer_No_Info_Records_Processed\",\n  \"systemIdentity\": {\n    \"ip\": \"10.0.0.1\"\n  },\n  \"properties\": {\n    \"report_name\": \"OMS_HR_Consumer_No_Info_Records_Processed\",\n    \"stream\": \"wcnp_opermnt\",\n    \"reason\": \"*** Sending Alert notification from OpenObserve Prod ****\",\n    \"url\": \"https://openlogsearch.logs.prod.walmart.com/api/default/short/3fcc8a3f6111913b\",\n    \"priority\": \"1\",\n    \"severity\": \"{severity}\",\n    \"tag\": \"oms-openobserve-alert\",\n    \"event_count\": \"100\",\n    \"start_time\": \"2025-03-20T07:08:03\",\n    \"end_time\": \"2025-03-20T07:09:04\",\n    \"custom_message\": \"{custom_message}\",\n    \"row_template\": \"WMPLTFMLOG541524\t1742454484962\t2025-03-20 07:08:04.962\t11.16.15.88\t-\t-\t-\t-\tprod\tOPERATOR-HR-TOPIC-CONSUMER\tuscentral-prod-az-011\tprod\t0.1.442\tffffffffab8be2a8-82-195b2616fe2006\tINFO\tINFO\t-\t-\t-\t-\tapplog.cls=com.walmart.operatorHRTopicConsumer.AssociateInfoConsumer,applog.mthd=processMessages,applog.line=132,applog.msg=Adding info record (winUser=****57881,country=US,storeId=6606) to batch\\nWMPLTFMLOG541524\t1742454484962\t2025-03-20 07:08:04.962\t11.16.15.88\t-\t-\t-\t-\tprod\tOPERATOR-HR-TOPIC-CONSUMER\tuscentral-prod-az-011\tprod\t0.1.442\tffffffffab8be2a8-82-195b2616fe2000\tINFO\tINFO\t-\t-\t-\t-\tapplog.cls=com.walmart.operatorHRTopicConsumer.AssociateInfoConsumer,applog.mthd=processMessages,applog.line=132,applog.msg=Adding info record (winUser=****55661,country=US,storeId=8294) to batch\\nWMPLTFMLOG541524\t1742454484962\t2025-03-20 07:08:04.962\t11.16.15.88\t-\t-\t-\t-\tprod\tOPERATOR-HR-TOPIC-CONSUMER\tuscentral-prod-az-011\tprod\t0.1.442\tffffffffab8be2a8-82-195b2616fe2007\tINFO\tINFO\t-\t-\t-\t-\tapplog.cls=com.walmart.operatorHRTopicConsumer.AssociateInfoConsumer,applog.mthd=processMessages,applog.line=132,applog.msg=Adding info record (winUser=****57977,country=US,storeId=6309) to batch\\nWMPLTFMLOG541524\t1742454484962\t2025-03-20 07:08:04.962\t11.16.15.88\t-\t-\t-\t-\tprod\tOPERATOR-HR-TOPIC-CONSUMER\tuscentral-prod-az-011\tprod\t0.1.442\tffffffffab8be2a8-82-195b2616fe2003\tINFO\tINFO\t-\t-\t-\t-\tapplog.cls=com.walmart.operatorHRTopicConsumer.AssociateInfoConsumer,applog.mthd=processMessages,applog.line=132,applog.msg=Adding info record (winUser=****55757,country=US,storeId=4785) to batch\\nWMPLTFMLOG541524\t1742454484962\t2025-03-20 07:08:04.962\t11.16.15.88\t-\t-\t-\t-\tprod\tOPERATOR-HR-TOPIC-CONSUMER\tuscentral-prod-az-011\tprod\t0.1.442\tffffffffab8be2a8-82-195b2616fe2004\tINFO\tINFO\t-\t-\t-\t-\tapplog.cls=com.walmart.operatorHRTopicConsumer.AssociateInfoConsumer,applog.mthd=processMessages,applog.line=132,applog.msg=Adding info record (winUser=****55949,country=US,storeId=3055) to batch\\nWMPLTFMLOG541524\t1742454484962\t2025-03-20 07:08:04.962\t11.16.15.88\t-\t-\t-\t-\tprod\tOPERATOR-HR-TOPIC-CONSUMER\tuscentral-prod-az-011\tprod\t0.1.442\tffffffffab8be2a8-82-195b2616fe2002\tINFO\tINFO\t-\t-\t-\t-\tapplog.cls=com.walmart.operatorHRTopicConsumer.AssociateInfoConsumer,applog.mthd=processMessages,applog.line=132,applog.msg=Adding info record (winUser=****55733,country=US,storeId=8188) to batch\\nWMPLTFMLOG541524\t1742454484961\t2025-03-20 07:08:04.961\t11.16.15.88\t-\t-\t-\t-\tprod\tOPERATOR-HR-TOPIC-CONSUMER\tuscentral-prod-az-011\tprod\t0.1.442\tffffffffab8be2a8-82-195b2616fe101e\tINFO\tINFO\t-\t-\t-\t-\tapplog.cls=com.walmart.operatorHRTopicConsumer.AssociateInfoConsumer,applog.mthd=processMessages,applog.line=132,applog.msg=Adding info record (winUser=****55565,country=US,storeId=6312) to batch\\nWMPLTFMLOG541524\t1742454484961\t2025-03-20 07:08:04.961\t11.16.15.88\t-\t-\t-\t-\tprod\tOPERATOR-HR-TOPIC-CONSUMER\tuscentral-prod-az-011\tprod\t0.1.442\tffffffffab8be2a8-82-195b2616fe101f\tINFO\tINFO\t-\t-\t-\t-\tapplog.cls=com.walmart.operatorHRTopicConsumer.AssociateInfoConsumer,applog.mthd=processMessages,applog.line=132,applog.msg=Adding info record (winUser=****55589,country=US,storeId=4926) to batch\\nWMPLTFMLOG541524\t1742454484962\t2025-03-20 07:08:04.962\t11.16.15.88\t-\t-\t-\t-\tprod\tOPERATOR-HR-TOPIC-CONSUMER\tuscentral-prod-az-011\tprod\t0.1.442\tffffffffab8be2a8-82-195b2616fe2001\tINFO\tINFO\t-\t-\t-\t-\tapplog.cls=com.walmart.operatorHRTopicConsumer.AssociateInfoConsumer,applog.mthd=processMessages,applog.line=132,applog.msg=Adding info record (winUser=****55685,country=US,storeId=7189) to batch\\nWMPLTFMLOG541524\t1742454484962\t2025-03-20 07:08:04.962\t11.16.15.88\t-\t-\t-\t-\tprod\tOPERATOR-HR-TOPIC-CONSUMER\tuscentral-prod-az-011\tprod\t0.1.442\tffffffffab8be2a8-82-195b2616fe2005\tINFO\tINFO\t-\t-\t-\t-\tapplog.cls=com.walmart.operatorHRTopicConsumer.AssociateInfoConsumer,applog.mthd=processMessages,applog.line=132,applog.msg=Adding info record (winUser=****57797,country=US,storeId=8285) to batch\\nWMPLTFMLOG541524\t1742454484961\t2025-03-20 07:08:04.961\t11.16.15.88\t-\t-\t-\t-\tprod\tOPERATOR-HR-TOPIC-CONSUMER\tuscentral-prod-az-011\tprod\t0.1.442\tffffffffab8be2a8-82-195b2616fe1015\tINFO\tINFO\t-\t-\t-\t-\tapplog.cls=com.walmart.operatorHRTopicConsumer.AssociateInfoConsumer,applog.mthd=processMessages,applog.line=132,applog.msg=Adding info record (winUser=****53285,country=US,storeId=6243) to batch\\nWMPLTFMLOG541524\t1742454484961\t2025-03-20 07:08:04.961\t11.16.15.88\t-\t-\t-\t-\tprod\tOPERATOR-HR-TOPIC-CONSUMER\tuscentral-prod-az-011\tprod\t0.1.442\tffffffffab8be2a8-82-195b2616fe1013\tINFO\tINFO\t-\t-\t-\t-\tapplog.cls=com.walmart.operatorHRTopicConsumer.AssociateInfoConsumer,applog.mthd=processMessages,applog.line=132,applog.msg=Adding info record (winUser=****53189,country=US,storeId=1856) to batch\\nWMPLTFMLOG541524\t1742454484961\t2025-03-20 07:08:04.961\t11.16.15.88\t-\t-\t-\t-\tprod\tOPERATOR-HR-TOPIC-CONSUMER\tuscentral-prod-az-011\tprod\t0.1.442\tffffffffab8be2a8-82-195b2616fe101c\tINFO\tINFO\t-\t-\t-\t-\tapplog.cls=com.walmart.operatorHRTopicConsumer.AssociateInfoConsumer,applog.mthd=processMessages,applog.line=132,applog.msg=Adding info record (winUser=****55517,country=US,storeId=8119) to batch\\nWMPLTFMLOG541524\t1742454484961\t2025-03-20 07:08:04.961\t11.16.15.88\t-\t-\t-\t-\tprod\tOPERATOR-HR-TOPIC-CONSUMER\tuscentral-prod-az-011\tprod\t0.1.442\tffffffffab8be2a8-82-195b2616fe1003\tINFO\tINFO\t-\t-\t-\t-\tapplog.cls=com.walmart.operatorHRTopicConsumer.AssociateInfoConsumer,applog.mthd=processMessages,applog.line=132,applog.msg=Adding info record (winUser=****51581,country=US,storeId=4471) to batch\\nWMPLTFMLOG541524\t1742454484961\t2025-03-20 07:08:04.961\t11.16.15.88\t-\t-\t-\t-\tprod\tOPERATOR-HR-TOPIC-CONSUMER\tuscentral-prod-az-011\tprod\t0.1.442\tffffffffab8be2a8-82-195b2616fe1005\tINFO\tINFO\t-\t-\t-\t-\tapplog.cls=com.walmart.operatorHRTopicConsumer.AssociateInfoConsumer,applog.mthd=processMessages,applog.line=132,applog.msg=Adding info record (winUser=****51653,country=US,storeId=3893) to batch\"\n  }\n}";
//     println!("{}", string);
// }

/*
Upcoming fire times:
-> 2018-06-01 09:30:00 UTC
-> 2018-06-01 12:30:00 UTC
-> 2018-06-01 15:30:00 UTC
-> 2018-06-15 09:30:00 UTC
-> 2018-06-15 12:30:00 UTC
-> 2018-06-15 15:30:00 UTC
-> 2018-08-01 09:30:00 UTC
-> 2018-08-01 12:30:00 UTC
-> 2018-08-01 15:30:00 UTC
-> 2018-08-15 09:30:00 UTC
*/
// fn main() {
//     let some_data = r#"{"name": "John", "age": 30, "city": "New York"}"#;
//     let ksuid = Ksuid::new_raw(0, Some(some_data.as_bytes()));
//     sleep(std::time::Duration::from_secs(5));
//     let ksuid2 = Ksuid::new_raw(0, Some(some_data.as_bytes()));
//     println!("{}", ksuid);
//     assert_eq!(ksuid.to_string(), ksuid2.to_string());
// }

fn main() -> std::io::Result<()> {
    // Generate approximately 5GB of data
    // Each row is roughly 100 bytes, so we need about 53,687,091 rows
    let target_rows = 15000;

    let mut rng = rand::rng();
    let start_date =
        NaiveDateTime::parse_from_str("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

    // Sample data arrays
    let names = [
        "John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Edward", "Fiona",
    ];
    let domains = ["gmail.com", "yahoo.com", "hotmail.com", "example.com"];
    let statuses = ["pending", "completed", "failed", "processing"];

    for i in 2..3 {
        // Create a file with buffered writer for better performance
        let file = File::create(format!("mb_data_{}.csv", i))?;
        let mut writer = BufWriter::with_capacity(1024 * 1024, file); // 1MB buffer

        // Write header
        writeln!(writer, "id,name,email,date,amount,status")?;
        for i in 0..target_rows {
            // Generate random data
            let name = names[rng.random_range(0..names.len())];
            let domain = domains[rng.random_range(0..domains.len())];
            let email = format!(
                "{}{}@{}",
                name.to_lowercase(),
                rng.random_range(100..999),
                domain
            );

            // Generate random date within last 3 years
            let days_offset = rng.random_range(0..1095); // 3 years in days
            let date = start_date + Duration::days(days_offset);

            // Generate random amount between 10.00 and 1000.00
            let amount = rng.random_range(1000..100000) as f64 / 100.0;

            let status = statuses[rng.random_range(0..statuses.len())];

            // Write row to file
            writeln!(
                writer,
                "{},{},{},{},{:.2},{}",
                i + 1,
                name,
                email,
                date.format("%Y-%m-%d %H:%M:%S"),
                amount,
                status
            )?;

            // Print progress every million rows
            if (i + 1) % 1_000_000 == 0 {
                println!("Generated {} million rows", (i + 1) / 1_000_000);
            }
        }
        writer.flush()?;
        println!("File generation completed! {}", i);
    }

    // Ensure all data is written
    Ok(())
}
