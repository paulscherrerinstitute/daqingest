use err::thiserror;
use err::ThisError;

#[derive(Debug, ThisError)]
#[cstm(name = "ConnChannelError")]
pub enum Error {}

trait Channel {
    fn can_accept_ca_msg(&self) -> bool;
    fn process_ca_msg(&mut self, msg: crate::ca::proto::CaMsg) -> Result<(), Error>;
}

struct ChannelAny {}
