enum EdgeType {

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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct S3Document {
    pub bucket: String,
    pub key: String
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UserData {
    pub name: Option<String>,
    pub given_name: Option<String>,
    pub surname: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SessionData {
    pub created: Option<String>,
    pub login: Option<String>,
    pub logout: Option<String>,
    pub auth_data: Option<String>
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
    pub s3: S3Document,
    pub owners: Vec<LegalEntity>,
    pub signatures: Vec<LegalEntity>,
    pub signature_reqs: Vec<LegalEntity>    
}
