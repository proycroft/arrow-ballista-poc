use anyhow::Result;
use ballista::prelude::*;
use datafusion::common::DataFusionError;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use deltalake::kernel::{DataType, MapType, PrimitiveType, StructField};
use deltalake::operations::convert_to_delta::ConvertToDeltaBuilder;
use deltalake::protocol::SaveMode;
use deltalake::writer::*;
use deltalake::{open_table, operations, DeltaOps, DeltaTableBuilder, Path};
use futures::future::join_all;
use parquet::basic::Compression;
use std::env;
use std::sync::Arc;
use std::time::Instant;
/*
use std::thread::*;
use url::*;
use ballista::prelude::BallistaError::DataFusionError;
use futures::prelude::*;
use rayon::prelude::*;
use object_store::azure::MicrosoftAzureBuilder;
 */

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    // These are needed client side
    // TODO: are they needed executor side? how is it passing this info around
/*    env::set_var("AZURE_STORAGE_ACCOUNT_NAME", "sparkpochdistorage");
    env::set_var(
        "AZURE_STORAGE_ACCOUNT_KEY",
    );
*/

    /*
    ConvertToDeltaBuilder::new()
        .with_location("/Users/proycroft/scratch/JAN-18856-large-parquet/KBXK19nYQu0MHsguq6Oa8gRBh8VHwgpoBYPg8uJjN5g_from_spark_repartition1/")
        .with_table_name("parquet-to-delta-test")
        .with_save_mode(SaveMode::Overwrite)
        .await.unwrap();
     */

    /*
    println!("try direct open table");
    let local_open_table = open_table("/Users/proycroft/scratch/JAN-18856-large-parquet/KBXK19nYQu0MHsguq6Oa8gRBh8VHwgpoBYPg8uJjN5g_from_spark_repartition1/").await.unwrap();
    println!("schema {:?}", local_open_table.schema());
     */

    deltalake_azure::register_handlers(None);

    /*
    // Read local delta table with datafusion

    println!("local building table");
    let mut local_table =
        DeltaTableBuilder::from_uri("azure://spark-poc-2024-01-24t18-53-06-978z/trydeltalake2/")
            //let mut local_table = DeltaTableBuilder::from_uri("azure://spark-poc-2024-01-24t18-53-06-978z/KBXK19nYQu0MHsguq6Oa8gRBh8VHwgpoBYPg8uJjN5g_from_spark_repartition1/")
            //let mut local_table = DeltaTableBuilder::from_uri("/Users/proycroft/scratch/JAN-18856-large-parquet/KBXK19nYQu0MHsguq6Oa8gRBh8VHwgpoBYPg8uJjN5g_from_spark_repartition1/")
            .with_allow_http(true)
            .build()
            .expect("table builder failed");

    local_table.load().await.unwrap();

    let delta_table_provider = Arc::new(local_table);

    println!("local register table");
    let local_ctx = datafusion::execution::context::SessionContext::new();
    local_ctx
        .register_table("delta_poc", delta_table_provider)
        .unwrap();

    // didnt help
    //local_ctx.refresh_catalogs().await.unwrap();

    println!("local make sql query");
    let local_df = local_ctx.sql("SELECT * FROM delta_poc LIMIT 1").await;
    if let Err(ref e) = local_df {
        println!("got a sql error {:?}", e)
    }

    local_df?.show().await.unwrap();

    */

    // Read using ballista

    let ctx = BallistaContext::remote(
        "localhost",
        //"ballista-scheduler",
        50050,
        &BallistaConfig::builder()
            //            .set("ballista.shuffle.partitions", "4")
            .build()
            .unwrap(),
        Arc::new(deltalake_core::delta_datafusion::DeltaLogicalCodec {}),
    )
    .await
    .unwrap();

    let start = Instant::now();

    // Manually uploaded the _delta_log dir from local conversion below to azure blob
    //let table_path = Path::parse("az://spark-poc-2024-01-24t18-53-06-978z/KBXK19nYQu0MHsguq6Oa8gRBh8VHwgpoBYPg8uJjN5g_from_spark_repartition1/").unwrap();
    //let table = deltalake::open_table(&table_path).await.unwrap();

    println!("building table");
    let mut table =
        DeltaTableBuilder::from_uri("azure://spark-poc-2024-01-24t18-53-06-978z/trydeltalake2/")
            //let mut table = DeltaTableBuilder::from_uri("azure://spark-poc-2024-01-24t18-53-06-978z/KBXK19nYQu0MHsguq6Oa8gRBh8VHwgpoBYPg8uJjN5g_from_spark_repartition1/")
            .with_allow_http(true)
            .build()
            .expect("table builder failed");

    table.load().await.unwrap();

    let delta_table_provider = Arc::new(table);

    println!("register table");
    ctx.register_table("delta_poc", delta_table_provider)
        .unwrap();

    println!("make sql query");
    let df = ctx.sql("SELECT * FROM delta_poc LIMIT 1").await;
    //let df = ctx.sql("SELECT count(*) FROM delta_poc LIMIT 1").await; // Also failing

    println!("Query took: {:?}", start.elapsed());

    if let Err(ref e) = df {
        println!("got a sql error {:?}", e)
    }

    // called `Result::unwrap()` on an `Err` value: Internal("failed to serialize logical plan: Context(\"Error serializing custom table at /usr/local/cargo/registry/src/index.crates.io-6f17d22bba15001f/datafusion-proto-35.0.0/src/logical_plan/mod.rs:1096\", NotImplemented(\"LogicalExtensionCodec is not provided\"))")

    //df.unwrap().collect().await.unwrap();

    df?.show().await.unwrap();

    /*
    // This seemed to work locally:
    ConvertToDeltaBuilder::new()
        // called `Result::unwrap()` on an `Err` value: InvalidTableLocation("Unknown scheme: azure")
        //.with_location("azure://spark-poc-2024-01-24t18-53-06-978z/KBXK19nYQu0MHsguq6Oa8gRBh8VHwgpoBYPg8uJjN5g_from_spark_repartition1/")
        .with_location("/Users/proycroft/scratch/JAN-18856-large-parquet/KBXK19nYQu0MHsguq6Oa8gRBh8VHwgpoBYPg8uJjN5g_from_spark_repartition1/")
        .with_table_name("parquet-to-delta-test")
        .await.unwrap();

     */

    /*
    let ops = DeltaOps::try_from_uri(table_path).await.unwrap();
    let mut table = ops
        .create()
        .with_columns(columns)
        .with_table_name("delta-poc")
        .await
        .unwrap();

     */

    Ok(())
}

/*
async fn run_query(ctx: &BallistaContext) -> Result<()> {
    // "ParentRemoteId" = 'uWg40P66WOzeHbjPLYlZlgvFje_c6hV6yBEprxFgv9vzt268i3VgpiELMijQI0uP1nMboT20jqMhGDbnZiS4FWoramX44UMctFExYShR-lkWsdb-kDD3qP_6' AND
    let results = ctx
        .sql(
            //r#"select count(*) from spark_poc"#,
            //r#"select * from spark_poc limit 500000"#,
            r#"
                   select * from (
                       select "RemoteId",
                       count(*) as count,
                       last_value("Deleted" order by "ManifestTime") as d,
                       last_value("DisplaySize" order by "ManifestTime") as ds,
                       last_value("Type" order by "ManifestTime") as type,
               last_value("DisplayName" order by "ManifestTime") as dn,
               last_value("ManifestTime" order by "ManifestTime")
               FROM spark_poc WHERE
               "ManifestTime" < '2024-02-13 00:13:09.582974'
               GROUP BY "RemoteId")
               where d!= true order by dn,"RemoteId" limit 30"#,
            //"ParentRemoteId" = 'uWg40P66WOzeHbjPLYlZlgvFje_c6hV6yBEprxFgv9vzt268i3VgpiELMijQI0uP1nMboT20jqMhGDbnZiS4FWoramX44UMctFExYShR-lkWsdb-kDD3qP_6' AND
        )
        //        .map_err(DataFusionError)
        //        .except("got sql error")
        .await;
    /*
       match results {
           Ok(DataFrame) => Ok(DataFrame),
           Err(DataFusionError(..)) => println!("got a sql error"),
       }
    */

    if let Err(ref e) = results {
        println!("got a sql error {:?}", e)
    }

    results?.show().await.unwrap();

    // Write out the results
    let write_res = results?.write_parquet(
            "azure://spark-poc-2024-01-24t18-53-06-978z/KBXK19nYQu0MHsguq6Oa8gRBh8VHwgpoBYPg8uJjN5g_from_ballista_500k",
            DataFrameWriteOptions::new()
                .with_single_file_output(true),
    //            .with_compression(CompressionTypeVariant::ZSTD),
            Option::from(
                WriterProperties::builder().set_compression(Compression::SNAPPY).build())
            )
            .await;

    if let Err(ref e) = write_res {
        println!("got an error writing parquet {e}");
    }

    Ok(())
}


 */
