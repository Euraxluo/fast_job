import os
import uuid
import ipaddress
from socket import gethostbyname, gethostname
from .about import NAME, VERSION, __version__
from .sonyflake import SonyFlake




def lower_16bit_ip_mac_pid() -> int:
    """
    :return:
    """
    ip: ipaddress.IPv4Address = ipaddress.ip_address(gethostbyname(gethostname()))
    ip_bytes = ip.packed
    res = hash(str([(ip_bytes[2] << 8) + ip_bytes[3], uuid.getnode(), os.getpid()])) & 0xffff
    return res


sony_flake = SonyFlake(machine_id=lower_16bit_ip_mac_pid)


def SonyFlakeId():
    return sony_flake.next_id()



__all__ = ["SonyFlakeId"]
