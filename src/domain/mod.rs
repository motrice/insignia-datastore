use std::str::FromStr;

pub enum EdgeType {
    SessionSelf,
    SessionUser,

    UserSelf, 
    UserPersonalNumber,
    UserEmail,
    UserPhone,
    //UserSession,
    
    DocumentSelf,
    DocumentOwner,
    DocumentReader,
    DocumentS3,
    DocumentChecksum,
    DocumentSignRequest,
    DocumentSignature
}

impl std::fmt::Display for EdgeType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            EdgeType::SessionSelf => write!(f, "session_self"),
            EdgeType::SessionUser => write!(f, "session_user"),

            EdgeType::UserSelf => write!(f, "usr_self"),
            EdgeType::UserPersonalNumber => write!(f, "usr_personal_number"),
            EdgeType::UserEmail => write!(f, "usr_email"),
            EdgeType::UserPhone => write!(f, "usr_phone"),
            //EdgeType::UserSession => write!(f, "session"),

            EdgeType::DocumentSelf => write!(f, "doc_self"),
            EdgeType::DocumentOwner => write!(f, "doc_acl_owner"),
            EdgeType::DocumentReader => write!(f, "doc_acl_reader"),
            EdgeType::DocumentS3 => write!(f, "doc_location_s3"),
            EdgeType::DocumentChecksum => write!(f, "doc_checksum"),
            EdgeType::DocumentSignRequest => write!(f, "doc_signreq"),
            EdgeType::DocumentSignature => write!(f, "doc_signature"),
        }
        
    }
}

impl FromStr for EdgeType {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let splitted: Vec<&str> = s.trim()
                                 .split('|')
                                 .collect();

        match splitted[0] {
           "session_self" => Ok(EdgeType::SessionSelf),
           "session_user" => Ok(EdgeType::SessionUser),
           "usr_self" => Ok(EdgeType::UserSelf),
           "usr_personal_number" => Ok(EdgeType::UserPersonalNumber),
           "usr_email" => Ok(EdgeType::UserEmail),
           "usr_phone" => Ok(EdgeType::UserPhone),
           "doc_self" => Ok(EdgeType::DocumentSelf),
           "doc_acl_owner" => Ok(EdgeType::DocumentOwner),
           "doc_acl_reader" => Ok(EdgeType::DocumentReader),
           "doc_location_s3" => Ok(EdgeType::DocumentS3),
           "doc_checksum" => Ok(EdgeType::DocumentChecksum),
           "doc_signreq" => Ok(EdgeType::DocumentSignRequest),
           "doc_signature" => Ok(EdgeType::DocumentSignature),
           _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid edge type"))
        }
    }
}



pub enum Vertex {
    User(String),
    Session(String),
    Document(String),
    DocumentS3(String),
    ChecksumSha256(String),
    PersonalNumber(String),
    Email(String),
    Phone(String)
}

impl std::fmt::Display for Vertex {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            Vertex::User(id) => write!(f, "User-{}", id),
            Vertex::Session(id) => write!(f, "Session-{}", id),
            Vertex::Document(id) => write!(f, "Document-{}", id),
            Vertex::DocumentS3(id) => write!(f, "S3-{}", id),
            Vertex::ChecksumSha256(sha) => write!(f, "SHA256-{}", sha),
            Vertex::PersonalNumber(personal_no) => write!(f, "PersonalNumber-{}", personal_no),
            Vertex::Email(email) => write!(f, "Email-{}", email),
            Vertex::Phone(phone) => write!(f, "Phone-{}", phone)
        }
        
    }
}

impl FromStr for Vertex {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let splitted: Vec<&str> = s.trim()
                                 .split('-')
                                 .collect();
        if splitted.len()<2 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid vertex format"));
        }
        match splitted[0] {
           "User" => Ok(Vertex::User(splitted[1..].join("-"))),
           "Session" => Ok(Vertex::Session(splitted[1..].join("-"))),
           "Document" => Ok(Vertex::Document(splitted[1..].join("-"))),
           "S3" => Ok(Vertex::DocumentS3(splitted[1..].join("-"))),
           "SHA256" => Ok(Vertex::ChecksumSha256(splitted[1..].join("-"))),
           "PersonalNumber" => Ok(Vertex::PersonalNumber(splitted[1..].join("-"))),
           "Email" => Ok(Vertex::Email(splitted[1..].join("-"))),
           "Phone" => Ok(Vertex::Phone(splitted[1..].join("-"))),
           _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid vertex type"))
        }
    }
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Edge {
    pub vertex_a: String,
    pub vertex_b: String,
    pub edge: String,
    pub data: Option<VertexData>
}

/*
impl Edge {
    fn new(vertex_a: &str, vertex_b: &str, edge: &str) -> Edge {
        Edge{
            vertex_a:String::from(vertex_a),
            vertex_b:String::from(vertex_b),
            edge:String::from(format!("{}|{}|{}", edge, vertex_a, vertex_b)),
        }
    }
}
*/

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum VertexData {
    S3Document(S3Document),
    String(String),
    UserData(UserData),
    SessionData(SessionData)
}

impl std::fmt::Display for VertexData {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            VertexData::S3Document(data) => write!(f, "User-{}", data),
            VertexData::String(data) => write!(f, "Session-{}", data),
            VertexData::UserData(data) => write!(f, "Document-{}", data),
            VertexData::SessionData(data) => write!(f, "S3-{}", data),
        }
        
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct S3Document {
    pub bucket: String,
    pub key: String
}

impl std::fmt::Display for S3Document {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "S3Document{{")?;
        write!(f, ", bucket: Some(\"{}\")", self.bucket)?;
        write!(f, ", key: Some(\"{}\")", self.key)?;
        write!(f, "}}")
    }
}
  


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UserData {
    pub name: Option<String>,
    pub given_name: Option<String>,
    pub surname: Option<String>,
}

impl std::fmt::Display for UserData {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "UserData{{")?;
        match &self.name {
            Some(s) => write!(f, ", name: Some(\"{}\")", s)?,
            None => write!(f, ", name: None")?
        };
        match &self.given_name {
            Some(s) => write!(f, ", given_name: Some(\"{}\")", s)?,
            None => write!(f, ", given_name: None")?
        };
        match &self.surname {
            Some(s) => write!(f, ", surname: Some(\"{}\")", s)?,
            None => write!(f, ", surname: None")?
        };
        write!(f, "}}")
    }
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SessionData {
    pub created: Option<String>,
    pub login: Option<String>,
    pub logout: Option<String>,
    pub auth_data: Option<String>
}

impl std::fmt::Display for SessionData {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SessionData{{")?;
        match &self.created {
            Some(s) => write!(f, ", created: Some(\"{}\")", s)?,
            None => write!(f, ", created: None")?
        };
        match &self.login {
            Some(s) => write!(f, ", login: Some(\"{}\")", s)?,
            None => write!(f, ", login: None")?
        };
        match &self.logout {
            Some(s) => write!(f, ", logout: Some(\"{}\")", s)?,
            None => write!(f, ", logout: None")?
        };
        match &self.auth_data {
            Some(s) => write!(f, ", auth_data: Some(\"{}\")", s)?,
            None => write!(f, ", auth_data: None")?
        };
        write!(f, "}}")
    }
}

pub struct Organisation {
    pub id: String,
    pub org_no: String,
    pub name: String
}

pub enum LegalEntity {
    Org(Organisation),
    User(User)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Session {
    pub session_id: String,
    pub created: Option<String>,
    pub login: Option<String>,
    pub logout: Option<String>,
    pub auth_data: Option<String>,
    pub user: Option<User>
}

impl Session {
    pub fn session_data(&self) -> SessionData {
        SessionData{
            created: self.created.clone(), 
            login: self.login.clone(),
            logout: self.logout.clone(),
            auth_data: self.auth_data.clone()
        }
    }
}

impl std::fmt::Display for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Session{{session_id: \"{}\"", self.session_id)?;
        match &self.created {
            Some(s) => write!(f, ", created: Some(\"{}\")", s)?,
            None => write!(f, ", created: None")?
        };
        match &self.login {
            Some(s) => write!(f, ", login: Some(\"{}\")", s)?,
            None => write!(f, ", login: None")?
        };
        match &self.logout {
            Some(s) => write!(f, ", logout: Some(\"{}\")", s)?,
            None => write!(f, ", logout: None")?
        };
        match &self.auth_data {
            Some(s) => write!(f, ", auth_data: Some(\"{}\")", s)?,
            None => write!(f, ", auth_data: None")?
        };
        match &self.user {
            Some(s) => write!(f, ", user: Some(\"{}\")", s)?,
            None => write!(f, ", user: None")?
        };
        write!(f, "}}")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct User {
    pub user_id: String,
    pub name: Option<String>,
    pub given_name: Option<String>,
    pub surname: Option<String>,
    pub personal_number: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>    
}

impl std::fmt::Display for User {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "User{{user_id: \"{}\"", self.user_id)?;
        match &self.name {
            Some(s) => write!(f, ", name: Some(\"{}\")", s)?,
            None => write!(f, ", name: None")?
        };
        match &self.given_name {
            Some(s) => write!(f, ", given_name: Some(\"{}\")", s)?,
            None => write!(f, ", given_name: None")?
        };
        match &self.surname {
            Some(s) => write!(f, ", surname: Some(\"{}\")", s)?,
            None => write!(f, ", surname: None")?
        };
        match &self.personal_number {
            Some(s) => write!(f, ", personal_number: Some(\"{}\")", s)?,
            None => write!(f, ", personal_number: None")?
        };
        match &self.email {
            Some(s) => write!(f, ", email: Some(\"{}\")", s)?,
            None => write!(f, ", email: None")?
        };
        match &self.phone {
            Some(s) => write!(f, ", phone: Some(\"{}\")", s)?,
            None => write!(f, ", phone: None")?
        };
        write!(f, "}}")
    }
}

pub struct Document {
    pub doc_id: String,
    pub owners: Vec<LegalEntity>,
    pub signatures: Vec<LegalEntity>,
    pub signature_reqs: Vec<LegalEntity>    
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DocumentReference {
    pub doc_id: String
}

impl std::fmt::Display for DocumentReference {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DocumentReference{{doc_id: \"{}\"", self.doc_id)?;
        write!(f, "}}")
    }
}
