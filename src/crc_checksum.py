class CRC16:
    @staticmethod
    def calc_next_crc_byte(crc_buff, nextbyte):
        for i in range(8):
            if (crc_buff & 0x0001) ^ (nextbyte & 0x01):
                x16 = 0x8408
            else:
                x16 = 0x0000
            crc_buff = crc_buff >> 1
            crc_buff ^= x16
            nextbyte = nextbyte >> 1
        return crc_buff

    @staticmethod
    def calc_crc16(data):
        crc = 0xFFFF
        for byte in data:
            crc = CRC16.calc_next_crc_byte(crc, byte)
        return crc
