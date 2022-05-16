#[derive(Debug)]
pub struct Counter {
    pub name: String,
}

#[derive(Debug)]
pub struct StatsStruct {
    pub name: String,
    pub counters: Vec<Counter>,
}

pub struct StatsStructDef {
    pub name: String,
    pub counters: Vec<Counter>,
}
