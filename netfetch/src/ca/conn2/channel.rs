use ca_proto::ca::proto;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "ConnChannelError")]
pub enum Error {}

trait Channel {
    fn can_accept_ca_msg(&self) -> bool;
    fn process_ca_msg(&mut self, msg: proto::CaMsg) -> Result<(), Error>;
}

struct ChannelAny {}
