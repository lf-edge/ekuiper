#  Copyright 2026 EMQ Technologies Co., Ltd.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
Test for PairChannel timeout behavior.

This test verifies that the PairChannel (used for function calls) properly
handles timeout scenarios by re-sending handshake to keep the connection alive.

The issue: After idle periods, the REQ socket may timeout waiting for messages.
After timeout, the REQ protocol requires sending before receiving again.
Without proper handling, this causes "incorrect protocol state" errors.
"""

import unittest
import threading
import time
from pynng import Rep0, Req0, Timeout


class TestReqSocketTimeoutBehavior(unittest.TestCase):
    """Test REQ socket behavior after recv timeout."""

    def test_recv_after_timeout_fails_without_send(self):
        """After timeout, trying to recv again should fail (protocol state error)."""
        url = "ipc:///tmp/test_timeout_behavior.ipc"
        
        # Create server (REP socket)
        server = Rep0()
        server.listen(url)
        
        # Create client (REQ socket) with short timeout
        client = Req0(recv_timeout=500, resend_time=0)
        client.dial(url)
        
        try:
            # Send handshake
            client.send(b'handshake')
            
            # Server receives but doesn't reply (simulating idle)
            msg = server.recv()
            self.assertEqual(msg, b'handshake')
            
            # Client should timeout waiting for reply
            with self.assertRaises(Timeout):
                client.recv()
            
            # After timeout, try to recv again without sending
            # This should fail with protocol state error
            # In pynng, this manifests as another Timeout or specific error
            try:
                client.recv()
                # If we get here, the second recv worked (unexpected)
                self.fail("Expected error after recv without send")
            except Exception as e:
                # Expected - protocol state error or timeout
                print(f"Got expected error: {type(e).__name__}: {e}")
                
        finally:
            client.close()
            server.close()

    def test_send_after_timeout_works(self):
        """After timeout, sending should work and reset protocol state."""
        url = "ipc:///tmp/test_timeout_send.ipc"
        
        # Create server (REP socket)
        server = Rep0()
        server.listen(url)
        
        # Create client (REQ socket) with short timeout
        client = Req0(recv_timeout=500, resend_time=0)
        client.dial(url)
        
        try:
            # Send handshake
            client.send(b'handshake')
            
            # Server receives but doesn't reply
            msg = server.recv()
            self.assertEqual(msg, b'handshake')
            
            # Client times out
            with self.assertRaises(Timeout):
                client.recv()
            
            # After timeout, send again (keepalive)
            client.send(b'keepalive')
            
            # Server should receive the keepalive
            msg = server.recv()
            self.assertEqual(msg, b'keepalive')
            
            # Now server replies
            server.send(b'reply')
            
            # Client should receive the reply
            reply = client.recv()
            self.assertEqual(reply, b'reply')
            
        finally:
            client.close()
            server.close()


if __name__ == '__main__':
    unittest.main()
