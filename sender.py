# sender.py is the sender side of the Reliable Calvin Message Protocol
#
# Usage: python3 sender.py server port filename
#        where server is IP address or hostname of the server,
#        port is an unused port number for the sender side,
#        and filename is the name of the file to transfer
#
# Author: Caleb Hurshman
# Date: 10/25/2021
# Based on the Reliable Calvin Message Protocol, outlined here:
# https://docs.google.com/document/d/19--3X5IwwDtJxeIE13WgtKm-gswR6fF8bejL468gIpo/edit#heading=h.r7wm0ewuayds
########################################################################################################################

from socket import *
from icecream import ic
import random
import os
import sys
import struct

PACKET_SIZE = 2048

# TO TOGGLE DEBUGGING, UNCOMMENT/COMMENT THE NEXT LINE
# ic.disable()


class NoData(Exception):
    """ Raised if sender tries to read data when there is none to be read """
    pass


class ReceiverNotResponding(Exception):
    """ Raised if 5 consecutive ACKs are not received """
    pass


class Sender:

    def __init__(self, server, port, filename):
        """
        Initialize a sender process with variables necessary for reliable file transfer
        Open the specified file for reading, open a UDP socket to the given port
        """
        self.server = server
        self.port = port
        self.filename = filename
        self.connection_id = random.randint(1, 15)   # generate unique connection id
        self.file_size = os.path.getsize(filename)   # find total size of file being read
        self.packet_index = 0                        # packet number, incremented for each new packet
        self.is_acked = False                        # ACK flag, 0 = Don't ACK, 1 = ACK
        self.ack_gap = 0                             # ACK gap counter, incremented for each received ACK
        self.last_acked_packet_num = 0               # variable to track last ACK'd packet number
        self.timeout_counter = 0                     # track the amount of times consecutive timeouts occur

        ic(self.connection_id)
        ic(filename, self.file_size)

    def start(self):
        """ Initialize a UDP socket and open the file, begin execution """
        self.receiver_addr = (self.server, self.port)
        self.sender_socket = socket(AF_INET, SOCK_DGRAM)
        self.sender_socket.settimeout(1)

        # open specified file for reading
        try:
            with open(self.filename, 'rb') as self.file:
                print("File", self.filename, "opened for reading")

                # Main loop
                while True:
                    try:
                        self.read_packet()
                        self.set_is_acked()
                        packet = self.build_packet()
                        self.send_packet(packet)
                        if self.is_acked:
                            self.await_ack()
                    except NoData:
                        break
                    except ReceiverNotResponding:
                        print("ReceiverNotResponding: 5 consecutive ACKs not received, exiting")
                        print("File transfer success unknown")
                        break
        finally:
            self.sender_socket.close()

    def read_packet(self):
        """ Read a packet from the file """
        self.data = self.file.read(PACKET_SIZE)  # read data from the file
        if not self.data:            # if there is no more data, don't try to send, break
            raise NoData

    def set_is_acked(self):
        """
        Determine whether a packet should be ACK'd or not, set the ACK flag accordingly
        if the amount of packets sent since the last ACK'd packet is the same as the ack_gap, ack the next packet
        """
        if self.packet_index - self.last_acked_packet_num == self.ack_gap:
            self.is_acked = 1
        else:
            self.is_acked = 0

    def build_packet(self):
        """
        Build a packet according to RCMP specifications
        https://docs.google.com/document/d/19--3X5IwwDtJxeIE13WgtKm-gswR6fF8bejL468gIpo/edit#heading=h.r7wm0ewuayds
        """
        return struct.pack("!3I?2048s",
                            self.connection_id,
                            self.file_size,
                            self.packet_index,
                            self.is_acked,
                            self.data)

    def send_packet(self, packet):
        """ Send a packet over the socket, increment packet number, wait for an ACK if necessary """
        try:
            self.sender_socket.sendto(packet, self.receiver_addr)
            self.packet_index += 1
            ic(self.is_acked)
        except Exception:
            print("Error while transmitting a packet")

    def await_ack(self):
        """
        Wait to receive ACK packet from the receiver, unpack the ACK
        Increment the gap between ACKs, reset the consecutive timeout counter,
            store the ACK packet number as the last ACK'd packet
        If a timeout occurs, enter packet loss recovery mode
        """
        try:
            raw_ack, addr = self.sender_socket.recvfrom(1024)
            unpacked_ack = struct.unpack("!2I", raw_ack)

            ack_connectionID, ack_packet_num = unpacked_ack

            ic(ack_connectionID, ack_packet_num)
            ic(self.ack_gap, self.last_acked_packet_num)
            ic("---------------------------------")

            self.ack_gap += 1                           # increment ACK gap by 1 for each received ACK
            self.timeout_counter = 0                    # reset the consecutive timeout counter to indicate receiver is responding
            self.last_acked_packet_num = ack_packet_num     # store the last ack'd packet
            # self.read_packet()                        # read next packet from file
        except timeout:
            self.packet_loss_recovery()

    def packet_loss_recovery(self):
        """
        Upon encountering a timeout while waiting for an ACK, enter packet loss recovery mode
        Use timeout counter to determine if the receiver is still responding
        Reset the gap between ACK'd packets to 0, reset the packet number to the first unACK'd packet
        Move file pointer back to start of last unACK'd packet
        """
        print("PacketLossDetected: Packet loss detected, beginning retransmission with packet", self.last_acked_packet_num)

        # check to see how many consecutive timeouts
        self.timeout_counter += 1

        if self.timeout_counter == 5:
            raise ReceiverNotResponding

        # start re-transmitting packets, starting with the one after the last ack'd packet
        self.ack_gap = 0     # reset ack-gap to 0
        self.packet_index = self.last_acked_packet_num    # reset packet number to beginning of retransmitted packets
            
        # reset file pointer to end of last ACK'd packet
        self.file.seek(self.last_acked_packet_num * PACKET_SIZE)
        #self.read_packet()  # start reading packets again


# Take in command line arguments
server = sys.argv[1]
portnum = sys.argv[2]
filename = sys.argv[3]

sender = Sender(server, int(portnum), filename)
sender.start()
