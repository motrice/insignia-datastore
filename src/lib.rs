extern crate rusoto_core;
extern crate rusoto_dynamodb;
extern crate uuid;
extern crate serde;
extern crate serde_dynamodb;
#[macro_use] extern crate serde_derive;
extern crate tokio;
extern crate chrono;
extern crate futures;

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
use anyhow::{Result, bail};

use log::{info, warn, error};

use uuid::Uuid;

pub mod domain;

use domain::*;

use std::collections::HashSet;

use futures::stream::{self, StreamExt};
use futures::future::join_all;

pub type AwsRegion = rusoto_core::Region;


/*
 TODO Major and important! Remove all sync. Perhaps wait until rusoto migrates to std::futures 
*/

pub fn new_edge(vertex_a: &Vertex, edge_type: &EdgeType, vertex_b: &Vertex, data: Option<VertexData>) -> Edge {
    Edge {
        vertex_a: vertex_a.to_string(),
        vertex_b: vertex_b.to_string(),
        edge: String::from(format!("{}|{}|{}", edge_type, vertex_a, &vertex_b)),
        data: data
    }
}


fn format_row(label: &str) -> String {
    format!("<tr><td colspan=\"2\" align=\"left\">{}</td></tr>", label)
}

fn format_row_attribute(key: &str, val: Option<String>) -> String {
    match val {
        Some(val) => format!("<tr><td align=\"left\">{}</td><td align=\"left\">{}</td></tr>", key, val),
        None => format!("<tr><td align=\"left\">{}</td></tr>", key),
    }
}

fn dot_format_s3_document(data: &S3Document) -> String {
    let mut res = format_row(&"<b>S3Document</b>");
    res.push_str(&format_row_attribute("bucket", Some(data.bucket.clone())));
    res.push_str(&format_row_attribute("key", Some(data.key.clone())));
    res.to_string()
}

fn dot_format_string(data: &str) -> String {
    let mut res = format_row("<b>String</b>");
    res.push_str(&format_row(data));

    res.to_string()
}

fn dot_format_user_data(data: &UserData) -> String {
    let mut res = format_row("<b>UserData</b>");
    res.push_str(&format_row_attribute("name", data.name.clone()));
    res.push_str(&format_row_attribute("given_name", data.given_name.clone()));
    res.push_str(&format_row_attribute("surname", data.surname.clone()));
    res.to_string()
}

fn dot_format_session_data(data: &SessionData) -> String {
    let mut res = format_row("<b>SessionData</b>");
    res.push_str(&format_row_attribute("created", data.created.clone()));
    res.push_str(&format_row_attribute("login", data.login.clone()));
    res.push_str(&format_row_attribute("logout", data.logout.clone()));
    res.push_str(&format_row_attribute("auth_data", data.auth_data.clone()));
    
    res.to_string()
}

fn format_vertex(data: &VertexData) -> String {
    match data { // TODO strange strings in write
        VertexData::S3Document(data) => dot_format_s3_document(data),
        VertexData::String(data) => dot_format_string(data),
        VertexData::UserData(data) => dot_format_user_data(data),
        VertexData::SessionData(data) => dot_format_session_data(data),
        VertexData::None => String::new()
    }
}


pub struct GraphDb {
    pub client: DynamoDbClient
}

impl GraphDb {


    pub fn new(region: Region) -> GraphDb {
        info!("Create dynamodb client");
        GraphDb {
            client: DynamoDbClient::new(region)
        }
    }

    pub fn new_with_region(region_name: &str, endpoint: &str) -> GraphDb {
        let region = Region::Custom {
                name: region_name.to_owned(),
                endpoint: endpoint.to_owned(),
        };
        info!("Create dynamodb client");
        GraphDb {
            client: DynamoDbClient::new(region)
        }
    }

    pub async fn create_table(&self) -> Result<()> {
        let create_res = self.client.create_table(CreateTableInput{
            table_name: String::from("insignia-docs"),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "vertex_a".into(),
                    key_type: "HASH".into(),
                }, 
                KeySchemaElement {
                    attribute_name: "edge".into(),
                    key_type: "RANGE".into(),
                }
            ],
            attribute_definitions: vec![
                AttributeDefinition {
                    attribute_name: "vertex_a".into(),
                    attribute_type: "S".into(),
                },
                AttributeDefinition {
                    attribute_name: "vertex_b".into(),
                    attribute_type: "S".into(),
                },
                AttributeDefinition {
                    attribute_name: "edge".into(),
                    attribute_type: "S".into(),
                }
            ],
            global_secondary_indexes: Some(vec![
                GlobalSecondaryIndex{
                    index_name: "index-vertex_b_edges".into(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "vertex_b".into(),
                            key_type: "HASH".into(),
                        }, 
                        KeySchemaElement {
                            attribute_name: "edge".into(),
                            key_type: "RANGE".into(),
                        }
                    ],
                    projection: Projection {
                        non_key_attributes: None,
                        projection_type: Some("ALL".into())

                    },
                    provisioned_throughput: Some(ProvisionedThroughput {
                        read_capacity_units: 1,
                        write_capacity_units: 1,
                    })
                }
            ]),
            provisioned_throughput: Some(ProvisionedThroughput {
                read_capacity_units: 1,
                write_capacity_units: 1,
            }),
            ..CreateTableInput::default()
        }).await?;
        
        match create_res.table_description {
            Some(desc) => println!("Created table {:?}", desc),
            None => println!("Created table, no table description")
        };
            
        Ok(())
    }

    pub async fn store_edge(&self, edge: &Edge) -> Result<()> {
        // todo retries etc due to documentation
        println!("Store edge {} {} {:?}", edge.vertex_a, edge.vertex_b, edge.data);
        let put_res = self.client.put_item(PutItemInput{
            condition_expression: None,
            conditional_operator: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            expected: None,
            item: serde_dynamodb::to_hashmap(&edge).unwrap(),
            return_values: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            table_name: String::from("insignia-docs")
        }).await?;
        info!("ok! {:?}", put_res);
        Ok(())
    }

    pub async fn get_vertex_with_edges(&self, vertex_id: &str) -> Result<Vec<Edge>> {
        // todo retries on error?? 
        let query_key_vertex_a: HashMap<String, AttributeValue> =
            [(String::from(":vertex_a"), AttributeValue{        
                    s:Some(String::from(vertex_id)),
                    ..Default::default()
                })]
            .iter().cloned().collect();

        let mut edges_from_vertex_a : Vec<Edge> = match self.client.query(
            QueryInput{
                table_name: String::from("insignia-docs"),
                key_condition_expression: Some(String::from("vertex_a = :vertex_a")),
                expression_attribute_values: Some(query_key_vertex_a),
                .. QueryInput::default()
            }).await {
                Ok(res) => {
                    res.items.unwrap_or_else(|| vec![]).into_iter().map(|item| serde_dynamodb::from_hashmap(item).unwrap()).collect()
                },
                Err(err) =>  {
                    error!("Error query{:?}", err);
                    vec![]
                }
        };

        let query_key_vertex_b: HashMap<String, AttributeValue> =
            [(String::from(":vertex_b"), AttributeValue{        
                    s:Some(String::from(vertex_id)),
                    ..Default::default()
                })]
            .iter().cloned().collect();

        let mut edges_from_vertex_b : Vec<Edge> = match self.client.query(
            QueryInput{
                table_name: String::from("insignia-docs"),
                key_condition_expression: Some(String::from("vertex_b = :vertex_b")),
                index_name: Some(String::from("index-vertex_b_edges")),
                expression_attribute_values: Some(query_key_vertex_b),
                .. QueryInput::default()
            }).await {
                Ok(res) => {
                    res.items.unwrap_or_else(|| vec![]).into_iter().map(|item| serde_dynamodb::from_hashmap(item).unwrap()).collect()
                },
                Err(err) =>  {
                    error!("Error query{:?}", err);
                    vec![]
                }
        };

        for item in &edges_from_vertex_a {
            info!("Edge {}  {} {} {:?}", item.vertex_a, item.edge, item.vertex_b, item.data);
        }
        for item in &edges_from_vertex_b {
            info!("GSI Edge {} {} {} {:?}", item.vertex_b, item.edge, item.vertex_a, item.data);
        }
        edges_from_vertex_a.append(&mut edges_from_vertex_b);
        Ok(edges_from_vertex_a)
    }

    pub async fn vertex_dot(&self, vertex_id: &str) -> Result<(String, String)> {
        let mut render_verticies : HashMap<String, VertexData> = HashMap::new();
        let mut render_edges : Vec<String> = Vec::new();

        let mut vertex_link_ids : HashSet<String> = HashSet::new();

        let edges = self.get_vertex_with_edges(&vertex_id).await?;
        for edge in edges {
            let key = edge.vertex_b;
            let key_a = edge.vertex_a;

            vertex_link_ids.insert(key.clone());
            vertex_link_ids.insert(key_a.clone());

            match edge.data {
                Some(data) => { 
                    render_verticies.insert(key.clone(), data.clone());
                },
                None => {
                    render_verticies.insert(key.clone(), VertexData::String(String::from("")));
                }
                
            }; 
            if !render_verticies.contains_key(&key_a) {
                render_verticies.insert(key_a.clone(), VertexData::String(String::from("")));
            }
            render_edges.push(format!(
                r#"{} -> {}"#, 
                &key_a.to_string().replace("-", "_").replace(".", "").replace("+", "").replace("@", ""), 
                &key.replace("-", "_").replace(".", "").replace("+", "").replace("@", "")
            )); 
        }

        let vertex_links = vertex_link_ids.iter().map(|vertex_key|format!(r#"<li><a href="?vertex-id={}">{}</a></li>"#, vertex_key, vertex_key)).collect::<Vec<String>>().join("");

        Ok((format!(r#"digraph {{ 
            node [shape=plaintext fontname="Sans serif" fontsize="8"];
            {}
            {}
            }}"#, 
            render_verticies.iter().map(|(key, value)|format!(r#" {} [ label=<
        <table border="1" cellborder="0" cellspacing="1" width="250">
        {}
        {}
        </table>>];
    "#, &key.replace("-", "_").replace(".", "").replace("+", "").replace("@", ""),
     &format_vertex(&value), 
     &format_row(&key))).collect::<Vec<String>>().join("\n"),
            render_edges.join("\n")),
            format!("<ul>{}</ul>", &vertex_links)))
    }

    pub async fn upload_document_url(&self, user_id: &Vertex) -> Result<String> {
        let doc_id = Vertex::Document(Uuid::new_v4().to_hyphenated().to_string());
        let edge = new_edge(&user_id, &EdgeType::DocumentOwner, &doc_id, Some(VertexData::String(String::from("some document data"))));
        self.store_edge(&edge).await?;
        Ok(format!("{}", &doc_id)) // TODO signed url
    }


    pub async fn upload_document_completed(&self, doc_id: &str, s3_bucket: &str, s3_key: &str, sha256:&str) -> Result<()> {
        let doc_id : Vertex = doc_id.parse()?;
        let s3_id = Vertex::DocumentS3(String::from(s3_key));
        let checksum_vertex = Vertex::ChecksumSha256(String::from(sha256));
        
        self.store_edge(
            &new_edge(
                &doc_id, 
                &EdgeType::DocumentSelf, 
                &doc_id, 
                Some(VertexData::String(String::from("some document data")))
            )
        ).await?;
        
        self.store_edge(
            &new_edge(
                &doc_id, 
                &EdgeType::DocumentS3, 
                &s3_id, 
                Some(VertexData::S3Document(S3Document{bucket: String::from(s3_bucket), key: String::from(s3_key)}))
            )
        ).await?;
        
        self.store_edge(
            &new_edge(
                &doc_id, 
                &EdgeType::DocumentChecksum, 
                &checksum_vertex, 
                None
            )
        ).await?;
        
        Ok(())
    }

    pub async fn new_user(&self, personal_number: &str, name: &str, given_name: &str, surname: &str, email:Option<&str>, phone:Option<&str>, session_id: Option<&str>) -> Result<String> {
        let user_id = Vertex::User(Uuid::new_v4().to_hyphenated().to_string());
        let pno_vertex = Vertex::PersonalNumber(String::from(personal_number));    

        self.store_edge(
            &new_edge(
                &user_id, 
                &EdgeType::UserSelf, 
                &user_id, 
                Some(VertexData::UserData(
                    UserData {
                        name: Some(String::from(name)),
                        surname: Some(String::from(surname)),
                        given_name: Some(String::from(given_name))
                    }
                ))
            )
        ).await?;

        self.store_edge(
            &new_edge(
                &user_id, 
                &EdgeType::UserPersonalNumber,
                &pno_vertex, 
                None
            )
        ).await?;        


        match email {
            Some(email) => {
                let email_vertex = Vertex::Email(String::from(email));
                self.store_edge(&new_edge(&user_id, &EdgeType::UserEmail, &email_vertex, None)).await?;
            },
            None => {}
        };

        match phone {
            Some(phone) => {
                let phone_vertex = Vertex::Phone(String::from(phone));
                self.store_edge(&new_edge(&user_id, &EdgeType::UserPhone, &phone_vertex, None)).await?;
            },
            None => {}
        };
        // match session_id {
        //     Some(session_id) => {
        //         let session_vertex = format!("Session-{}", session_id);
        //         result.push(new_edge(&user_id, "session", &session_vertex, None))
        //     },
        //     None => {}
        // }

        Ok(user_id.to_string())
    }

    pub async fn session_new(&self) -> Result<Session> {
        let session_vertex = Vertex::Session(Uuid::new_v4().to_hyphenated().to_string());
        let now: DateTime<Utc> = Utc::now();
        let created = Some(now.to_rfc3339());

        self.store_edge(
            &new_edge(
                &session_vertex, 
                &EdgeType::SessionSelf, 
                &session_vertex, 
                Some(VertexData::SessionData(SessionData{created: created.clone(), session_login_id: None, login: None, logout: None, auth_data: None}))
            )
        ).await?;
        
        Ok(
            Session{
                session_id: session_vertex.to_string(), 
                created: created, 
                session_login_id: None, 
                login: None, 
                logout: None, 
                user: None, 
                auth_data: None
            }
        )
    }

    pub async fn session_auth(&self, session_id: &str, user_id: &str, auth_data: &str) -> Result<Session> {
        let now: DateTime<Utc> = Utc::now();
        let login = Some(now.to_rfc3339());
        let session_vertex = session_id.parse()?;
        let login_vertex = Vertex::SessionLogin(Uuid::new_v4().to_hyphenated().to_string());
        let session_login_id = Uuid::new_v4().to_hyphenated().to_string();

        match self.get_user(user_id).await {
            Some(user) => {
                let user_vertex = user_id.parse()?;
                let sessions = self.sessions_get(session_id).await;
                match sessions.len() {
                    0 => {
                            bail!("Invalid session_id")
                        },
                    _ => {
                            let session = &sessions[0];
                            let session_data = VertexData::SessionData(SessionData{login: login.clone(), session_login_id: Some(login_vertex.to_string()), auth_data: Some(String::from(auth_data)), ..session.session_data()});
                            self.store_edge(
                                &new_edge(
                                    &session_vertex, 
                                    &&EdgeType::SessionUser, 
                                    &user_vertex, 
                                    None
                                )
                            ).await?;
                            self.store_edge(
                                &new_edge(
                                    &session_vertex, 
                                    &EdgeType::SessionLogin, 
                                    &login_vertex, 
                                    Some(session_data.clone())
                                )
                            ).await?;

                            Ok(
                                Session{
                                    login: login, 
                                    user: Some(user), 
                                    ..session.clone()
                                }
                            )
                        } 
                }
            },
            None => bail!("Invalid user_id")
        }
    }

    pub async fn session_logout(&self, session_id: &str) -> Result<()> {
        let now: DateTime<Utc> = Utc::now();
        let logout = Some(now.to_rfc3339());
        let session_vertex = session_id.parse()?;

        let sessions: Vec<Session> = self.sessions_get(session_id).await;
        let filtered_sessions = sessions
        .iter()
        .filter(|item| match &item.session_login_id {
            Some(_) => true,
            None => false
        });

        for session in &sessions {
            match &session.session_login_id {
                Some(session_login_id) => {
                    println!("Parse session_login_id --->{}<---", session_login_id);
                    match self.store_edge(&new_edge(
                    &session_vertex, 
                    &EdgeType::SessionLogout, 
                    &session_login_id.parse::<Vertex>().unwrap(), 
                    Some(VertexData::SessionData(SessionData{logout: logout.clone(), ..session.session_data()}))
                    )).await {
                        Ok(_) => info!("Logged out session {}", session),
                        Err(err) => error!("Could not logout session {} {}", session, err)
                    };
                },
                None => info!("No session_login_id to logout")
            }
        }

        Ok(()) 
    }

    pub async fn sessions_get(&self, session_id: &str) -> Vec<Session> {
        let query_key_vertex_a: HashMap<String, AttributeValue> =
            [(String::from(":vertex_a"), AttributeValue{        
                    s:Some(String::from(session_id)),
                    ..Default::default()
                }),
            (String::from(":userdata_prefix"), AttributeValue{        
                    s:Some(String::from("session_")),
                    ..Default::default()
                })]
            .iter().cloned().collect();

        let edges_from_vertex_a : Vec<Edge> = match self.client.query(
            QueryInput{
                table_name: String::from("insignia-docs"),
                key_condition_expression: Some(String::from("vertex_a = :vertex_a and begins_with(edge, :userdata_prefix)")),
                expression_attribute_values: Some(query_key_vertex_a),
                .. QueryInput::default()
            }).await {
                Ok(res) => {
                    res.items.unwrap_or_else(|| vec![]).into_iter().map(|item| serde_dynamodb::from_hashmap(item).unwrap()).collect()
                },
                Err(err) =>  {
                    vec![]
                }
        };

        if edges_from_vertex_a.len() == 0 {
            return Vec::new();
        }

        let mut created : Option<String> = None;
        let mut login: Option<String> = None;
        let mut logout: Option<String> = None;
        let mut auth_data: Option<String> = None;
        let mut user : Option<User> = None;

        let mut session_logins: HashMap<String, VertexData> = HashMap::new();
        let mut session_logouts: Vec<String> = Vec::new();
        
        for item in &edges_from_vertex_a {
            let splitted : Vec<&str> = item.vertex_b.split("-").collect();
            if splitted.len()>1 {
                match splitted[0] {
                    "User" => {
                        user = self.get_user(&item.vertex_b).await;
                    },
                    "SessionLogin" => {
                        println!("======> SessionLogin EDGE {:?} [[[vertex_a: {}]]] [[[vertex_b: {}]]] vertex_a: DATA {:?}", item, item.vertex_a, item.vertex_b, item.data);
                        match &item.data {
                            Some(data) => {
                                match data {
                                    VertexData::SessionData(session_data) => {
                                        match &session_data.login {
                                    
                                            Some(login_data) => match session_logins.insert(item.vertex_b.clone(), data.clone()) {
                                                Some(v) => error!("Unexpected duplicate value {}", v),
                                                None => ()
                                            },
                                            None => ()
                                        }
                                        match &session_data.logout {
                                            Some(logout_data) => {
                                                session_logouts.push(item.vertex_b.clone())
                                            },
                                            None => ()
                                        }
                                    },
                                    _ => ()
                                }
                            },
                            None => ()

                        }
                        
                    },  
                    "Session" => {
                        match &item.data {
                            Some(data) => match data {
                                VertexData::SessionData(session_data) => {
                                    created = session_data.created.clone();
                                    ()
                                },
                                _ => () 
                            },
                            None => ()
                        }
                    },
                    _ => info!("Unknown session property {}", &item.vertex_b)
                    
                }
            }
        }

        for session_logout in &session_logouts {
            session_logins.remove(session_logout);
        }

        if session_logins.len() == 0 {
            println!("WHAT NO LOGINS");
            return vec![
                    Session {
                        session_id: String::from(session_id),
                        created: created,
                        session_login_id: None,
                        login: None,
                        logout: None,
                        auth_data: None,
                        user: user
                    }
                ];
        }

        session_logins.iter().filter_map(|(session_login, vertex_data)|{
            match vertex_data {
                VertexData::SessionData(session_data) => Some(Session {
                    session_id: String::from(session_id),
                    session_login_id: Some(session_login.clone()),
                    created: created.clone(),
                    login: session_data.login.clone(),
                    logout: session_data.logout.clone(),
                    auth_data: session_data.auth_data.clone(),
                    user: user.clone()
                }),
                _=> None
            }
        }).collect()
    }

    pub async fn get_user(&self, user_id: &str) -> Option<User> {
        let query_key_vertex_a: HashMap<String, AttributeValue> =
            [(String::from(":vertex_a"), AttributeValue{        
                    s:Some(String::from(user_id)),
                    ..Default::default()
                }),
            (String::from(":userdata_prefix"), AttributeValue{        
                    s:Some(String::from("usr_")),
                    ..Default::default()
                })]
            .iter().cloned().collect();

        let edges_from_vertex_a : Vec<Edge> = match self.client.query(
            QueryInput{
                table_name: String::from("insignia-docs"),
                key_condition_expression: Some(String::from("vertex_a = :vertex_a and begins_with(edge, :userdata_prefix)")),
                expression_attribute_values: Some(query_key_vertex_a),
                .. QueryInput::default()
            }).await {
                Ok(res) => {
                    res.items.unwrap_or_else(|| vec![]).into_iter().map(|item| serde_dynamodb::from_hashmap(item).unwrap()).collect()
                },
                Err(err) =>  {
                    error!("Error query{:?}", err);
                    vec![]
                }
        };

        if edges_from_vertex_a.len() == 0 {
            return None;
        }

        let mut name : Option<String> = None;
        let mut given_name: Option<String> = None;
        let mut surname: Option<String> = None;
        let mut personal_number: Option<String> = None;
        let mut email: Option<String> = None;
        let mut phone: Option<String> = None;

        for item in &edges_from_vertex_a {
            let splitted : Vec<&str> = item.vertex_b.split("-").collect();
            if splitted.len()>1 {
                match splitted[0] {
                    "Email" => {
                        email = Some(splitted[1..].join("-"));
                    },
                    "PersonalNumber" => {
                        personal_number = Some(splitted[1..].join("-"));
                    },
                    "Phone" => {
                        phone = Some(splitted[1..].join("-"));
                    },
                    "User" => {
                        match &item.data {
                            Some(data) => match data {
                                VertexData::UserData(user_data) => {
                                    name = user_data.name.clone();
                                    given_name = user_data.given_name.clone();
                                    surname = user_data.surname.clone();
                                    ()
                                },
                                _ => () 
                            },
                            None => ()
                        }
                    },
                    _ => info!("Unknown user property {}", &item.vertex_b)
                    
                }
            }
        }

        Some(User{
            user_id: String::from(user_id),
            name: name,
            given_name: given_name,
            surname: surname,
            personal_number: personal_number,
            email: email,
            phone: phone
        })
    }

    pub async fn get_user_documents(&self, user_id: &str) -> Vec<DocumentReference> {
        let query_key_vertex_a: HashMap<String, AttributeValue> =
            [(String::from(":vertex_a"), AttributeValue{        
                    s:Some(String::from(user_id)),
                    ..Default::default()
                }),
            (String::from(":userdocs"), AttributeValue{        
                    s:Some(String::from("doc_acl_")), 
                    ..Default::default()
                })]
            .iter().cloned().collect();

        let edges_from_vertex_a : Vec<Edge> = match self.client.query(
            QueryInput{
                table_name: String::from("insignia-docs"),
                key_condition_expression: Some(String::from("vertex_a = :vertex_a and begins_with(edge, :userdocs)")),
                expression_attribute_values: Some(query_key_vertex_a),
                .. QueryInput::default()
            }).await {
                Ok(res) => {
                    res.items.unwrap_or_else(|| vec![]).into_iter().map(|item| serde_dynamodb::from_hashmap(item).unwrap()).collect()
                },
                Err(err) =>  {
                    error!("Error query{:?}", err);
                    vec![]
                }
        };

        if edges_from_vertex_a.len() == 0 {
            return vec![];
        }

        edges_from_vertex_a.iter().map(|item|DocumentReference{doc_id: item.vertex_b.clone()}).collect::<Vec<DocumentReference>>()
    }



    pub async fn get_users_by_personal_number(&self, personal_number: &str) -> Vec<User> {
        let query_key_vertex_b: HashMap<String, AttributeValue> =
            [(String::from(":vertex_b"), AttributeValue{        
                    s:Some(format!("PersonalNumber-{}", personal_number)),
                    ..Default::default()
                })]
            .iter().cloned().collect();

        let edges_from_vertex_b : Vec<Edge> = match self.client.query(
            QueryInput{
                table_name: String::from("insignia-docs"),
                key_condition_expression: Some(String::from("vertex_b = :vertex_b")),
                index_name: Some(String::from("index-vertex_b_edges")),
                expression_attribute_values: Some(query_key_vertex_b),
                .. QueryInput::default()
            }).await {
                Ok(res) => {
                    res.items.unwrap_or_else(|| vec![]).into_iter().map(|item| serde_dynamodb::from_hashmap(item).unwrap()).collect()
                },
                Err(err) =>  {
                    error!("Error query{:?}", err);
                    vec![]
                }
        };
        let userFutures = edges_from_vertex_b.iter().map(
            |itm| self.get_user(&itm.vertex_a)
        );
        let users: Vec<Option<User>> = join_all(userFutures).await; 

        users.iter().filter(
            |itm| match itm { 
                Some(_)=> true, 
                None=> false
            }
        ).map(
            |itm| itm.clone().unwrap()
        ).collect::<Vec<User>>()
    }

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

doc-nnnn    doc-nnnn        "title: String, checksum: String, created, modified"
doc-nnnn    s3-ref
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


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
        fn one_result() -> Result<()> {

            Ok(())
        }
}
