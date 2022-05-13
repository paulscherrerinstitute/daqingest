use proc_macro::{Delimiter, TokenStream, TokenTree};

#[derive(Debug)]
struct Counter {
    name: String,
}

#[derive(Debug)]
struct StatsStruct {
    name: String,
    counters: Vec<Counter>,
}

fn parse_name(gr: proc_macro::Group) -> String {
    for tok in gr.stream() {
        match tok {
            TokenTree::Ident(k) => {
                return k.to_string();
            }
            _ => {}
        }
    }
    panic!();
}

fn parse_counters(gr: proc_macro::Group, a: &mut Vec<Counter>) {
    for tok in gr.stream() {
        match tok {
            TokenTree::Ident(k) => {
                let x = Counter { name: k.to_string() };
                a.push(x);
            }
            _ => {}
        }
    }
}

fn stats_struct_agg_impl(st: &StatsStruct) -> String {
    let name = format!("{}Agg", st.name);
    let counters_decl: Vec<_> = st
        .counters
        .iter()
        .map(|x| format!("pub {}: AtomicU64", x.name))
        .collect();
    let counters_decl = counters_decl.join(",\n");
    let inits: Vec<_> = st
        .counters
        .iter()
        .map(|x| format!("{}: AtomicU64::new(0)", x.name))
        .collect();
    let inits = inits.join(",\n");
    format!(
        "
pub struct {name} {{
    pub ts_create: Instant,
    pub aggcount: AtomicU64,
    {counters_decl},
}}

impl {name} {{
    pub fn new() -> Self {{
        Self {{
            ts_create: Instant::now(),
            aggcount: AtomicU64::new(0),
            {inits},
        }}
    }}
}}
    "
    )
}

fn stats_struct_impl(st: &StatsStruct) -> String {
    let name = &st.name;
    let inits: Vec<_> = st
        .counters
        .iter()
        .map(|x| format!("{}: AtomicU64::new(0)", x.name))
        .collect();
    let inits = inits.join(",\n");
    format!(
        "
impl {name} {{
    pub fn new() -> Self {{
        Self {{
            ts_create: Instant::now(),
            {inits}
        }}
    }}
}}
    "
    )
}

fn make_code(st: &StatsStruct) -> String {
    let counters_decl: Vec<_> = st
        .counters
        .iter()
        .map(|x| format!("pub {}: AtomicU64", x.name))
        .collect();
    let counters_decl = counters_decl.join(",\n");
    let structt = format!(
        "
pub struct {} {{
    pub ts_create: Instant,
    {},
}}

",
        st.name, counters_decl
    );
    let code = format!(
        "{}\n\n{}\n\n{}",
        structt,
        stats_struct_impl(st),
        stats_struct_agg_impl(st)
    );
    code
}

#[proc_macro]
pub fn stats_struct(ts: TokenStream) -> TokenStream {
    use std::fmt::Write;
    let mut counters = vec![];
    let mut log = String::new();
    let mut in_name = false;
    let mut in_counters = false;
    let mut name = None;
    for tt in ts.clone() {
        match tt {
            TokenTree::Ident(k) => {
                if k.to_string() == "name" {
                    in_name = true;
                }
                if k.to_string() == "counters" {
                    in_counters = true;
                }
            }
            TokenTree::Group(k) => {
                if in_counters {
                    in_counters = false;
                    parse_counters(k, &mut counters);
                } else if in_name {
                    in_name = false;
                    name = Some(parse_name(k));
                }
            }
            TokenTree::Punct(..) => (),
            TokenTree::Literal(k) => {
                if in_name {
                    write!(log, "NAME write literal {}\n", k);
                    name = Some(k.to_string());
                }
            }
        }
    }
    let name = name.unwrap();
    let stats_struct = StatsStruct { name, counters };
    write!(log, "{:?}\n", stats_struct);
    //panic!("{}", make_code(&stats_struct));
    //panic!("{}", log);
    let ts2 = TokenStream::from_iter(ts.into_iter());
    TokenTree::Group(proc_macro::Group::new(Delimiter::Brace, TokenStream::new()));
    make_code(&stats_struct).parse().unwrap()
}
