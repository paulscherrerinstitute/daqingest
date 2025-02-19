use ca_proto::ca::proto;

autoerr::create_error_v1!(
    name(Error, "ConnChannelError"),
    enum variants {
        Logic,
    },
);

trait Channel {
    fn can_accept_ca_msg(&self) -> bool;
    fn process_ca_msg(&mut self, msg: proto::CaMsg) -> Result<(), Error>;
}

struct ChannelAny {}
