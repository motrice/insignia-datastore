extern crate rusoto_core;
extern crate rusoto_dynamodb;
extern crate uuid;
extern crate serde;
extern crate serde_dynamodb;
#[macro_use] extern crate serde_derive;
extern crate tokio;
extern crate chrono;
use chrono::{DateTime, Utc};

use std::collections::HashMap;
use rusoto_core::Region;
use rusoto_dynamodb::{
    AttributeDefinition, 
    AttributeValue,
    CreateTableInput, 
    DeleteTableInput, 
    DynamoDb, 
    DynamoDbClient, 
    GetItemInput,
    GlobalSecondaryIndex,
    KeySchemaElement, 
    ListTablesInput, 
    Projection,
    ProvisionedThroughput,
    PutItemInput,
    QueryInput,
    ScanInput
};
use anyhow::{Result};

use uuid::Uuid;

pub mod domain;

use insignia_datastore::domain::*;

use insignia_datastore::*;

// pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
// pub type Result<T> = std::result::Result<T, GenericError>;


#[tokio::main]
pub async fn main() -> Result<()> {
    

        println!("Hello, world!");

    let region = Region::Custom {
        name: "eu-north-1".to_owned(),
        endpoint: "http://localhost:8000".to_owned(),
    };

    let client = insignia_datastore::GraphDb::new_with_region("eu-north-1", "http://localhost:8000");

    match client.client.delete_table(DeleteTableInput{table_name: String::from("bm-test-table")}).await {
        Ok(_) => println!("Deleted table"),
        Err(_) => println!("Delete failed")
    }
/*
user-nnnn   user-nnnn       givenName, surname
user-nnnn   personalNumber  personalNumber
            email
            phone
            adress
            name            name
org-nnnn    org-nnnn        "org data"
org-nnnn    user-nnnn       "org user role"

doc-nnnn    doc-nnnn        "title: String, , created, modified"
doc-nnnn    s3-ref
doc-nnnn    checksum          
doc-nnnn    user-nnnn       "user role"
doc-nnnn    org-nnnn        "org role"
doc-nnnn    signreq-nnnn    "sign req"
doc-nnnn    signature-nnnn  "signature data"
doc-nnnn    link            "permissons"

{
  "IndexName" : String,
  "KeySchema" : [ KeySchema, ... ],
  "Projection" : Projection,
  "ProvisionedThroughput" : ProvisionedThroughput
}

*/
    client.create_table().await?;

    let mut user1 = client.new_user("191212121212", "Tolvan Tolvansson", "Tolvan", "Tolvansson", Some("tolvan.tolvansson@motrice.se"), Some("+46733414983"), None).await?;
    
    match client.upload_document_url("motrice-insignia", &user1.parse()?).await {
        Ok(todo_doc_id) => {
            match client.upload_document_completed(&todo_doc_id, "foobucket", "barkey", "1234checksum").await {
                Ok(_) => println!("upload doc completed"),
                Err(err) => println!("upload doc err {}", err)
            }
        },
        Err(err) => println!("Error {}", err)
    }

    let scan_edges : Vec<Edge> = match client.client.scan(ScanInput{
        table_name: String::from("insignia-docs"),
        ..ScanInput::default()
    }).await {
            Ok(res) => {
                res.items.unwrap_or_else(|| vec![]).into_iter().map(|item| serde_dynamodb::from_hashmap(item).unwrap()).collect()
            },
            Err(err) =>  {
                println!("Error query{:?}", err);
                vec![]
            }
    };

    for item in &scan_edges {
        println!("Scan Edge {} {} {} {:?}", item.vertex_b, item.edge, item.vertex_a, item.data);
    }

    for item in &scan_edges {
        let edges = client.get_vertex_with_edges(&item.vertex_a).await?;
        println!("Vertex: {}", &item.vertex_a);
        for edge in &edges {
            //println!("    Edge {} {} {} {:?}", edge.vertex_b, edge.edge, edge.vertex_a, edge.data);
            println!("    Edge {} {:?}", edge.edge, edge.data);
            
        }
    }

    let users = client.get_users_by_personal_number("191212121212").await;
    for item in &users {
        println!("Tolvan user: {}", item);
    }

    match client.session_new().await {
        Ok(session) => {
            println!("new session:            {}", &session);
            let sessions = client.sessions_get(&session.session_id).await;
            for loaded_sess in &sessions {
                println!("Loaded the new session: {}", &loaded_sess);
            }
            match client.session_auth(&session.session_id, &users[0].user_id, "logged in from ip 123.456.7.8").await {
                Ok(sess_auth) =>  {
                    println!("auth session:            {}", &session);
                    let sessions =  client.sessions_get(&sess_auth.session_id).await;
                    for loaded_sess in &sessions {
                        println!("Loaded the new session auth: {}", &loaded_sess);
                    }
                    println!("logout session:            {}", &session);
                    match client.session_logout(&sess_auth.session_id).await {
                        Ok(_) => {
                            let sessions =  client.sessions_get(&sess_auth.session_id).await;
                            for loaded_sess in &sessions {
                                println!("Loaded the new session logout: {}", &loaded_sess);
                            }
                        },
                        Err(err) => println!("Error during session logout {}", err)        
                    }

                },
                Err(err) => println!("Error during session auth {}", err)        
            }
        }, 
        Err(err) => println!("Error while creating new session {}", err)
    }

    match client.upload_document_url("motrice-insignia", &users[0].user_id.parse()?).await {
        Ok(todo_doc_id) => {
            match client.upload_document_completed(&todo_doc_id, "somebucketname", "somekeyval", "somesha").await {
                Ok(_) => println!("upload doc completed"),
                Err(err) => println!("upload doc err {}", err)
            }
        },
        Err(err) => println!("Error {}", err)
    }

    let user_documents =  client.get_user_documents(&users[0].user_id).await;
    for item in &user_documents {
        println!("User document: {}", &item);
    }

    match client.vertex_dot(&users[0].user_id.to_string()).await {
        Ok((dot, links)) => println!("dot: {} {}", dot, links),
        Err(err) => println!("upload doc err {}", err)
    };
    Ok(())
    }
