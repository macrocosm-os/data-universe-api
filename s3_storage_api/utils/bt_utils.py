"""
Bittensor utility functions for blockchain commitment verification
"""
import time
import bittensor as bt
from typing import Optional
from bittensor import Keypair



def is_hotkey_registered(hotkey: str, netuid: int, network: str) -> bool:
    """
    Check if a hotkey is registered in the metagraph for a given subnet.
    """
    subtensor = get_subtensor(network)
    if not subtensor:
        return False
    try:
        metagraph = subtensor.metagraph(netuid=netuid)
        return hotkey in metagraph.hotkeys
    except Exception as e:
        print(f"Error checking hotkey registration: {str(e)}")
        return False


def verify_signature(message: str, signature_hex: str, hotkey_ss58: str, netuid: int, network: str) -> bool:
    """
    Verify that the message was signed by the hotkey.
    Also ensures the hotkey is registered in the metagraph.
    """
    try:
        if not is_hotkey_registered(hotkey_ss58, netuid, network):
            print(f"Hotkey {hotkey_ss58} is not registered in subnet {netuid}")
            return False
        
        
        kp = Keypair(ss58_address=hotkey_ss58)
        signature = bytes.fromhex(signature_hex)
        return kp.verify(message.encode(), signature)
    except Exception as e:
        print(f"Signature verification error: {e}")
        return False



def get_subtensor(network="finney"):
    """Get Bittensor subtensor connection"""
    try:
        return bt.subtensor(network=network)
    except Exception as e:
        print(f"Error connecting to Bittensor network: {str(e)}")
        return None


def get_commitment(hotkey: str, netuid: int, network='finney') -> Optional[str]:
    """Get the latest commitment from the blockchain"""
    subtensor = get_subtensor(network)
    if not subtensor:
        return None

    try:
        # Get UID for hotkey
        uid = subtensor.get_uid_for_hotkey_on_subnet(hotkey_ss58=hotkey, netuid=netuid)
        if uid is None:
            print(f"Hotkey {hotkey} not registered on subnet {netuid}")
            return None

        # Get commitment
        commitment = subtensor.get_commitment(netuid=netuid, uid=uid)
        return commitment
    except Exception as e:
        print(f"Error getting commitment: {str(e)}")
        return None


def verify_validator_status(hotkey: str, netuid: int, network: str) -> bool:
    """Check if a hotkey belongs to a validator with a permit"""
    subtensor = get_subtensor(network)
    if not subtensor:
        return False

    try:
        # Get UID for hotkey
        uid = subtensor.get_uid_for_hotkey_on_subnet(hotkey_ss58=hotkey, netuid=netuid)
        if uid is None:
            return False

        # Get metagraph to check validator permit
        metagraph = subtensor.metagraph(netuid=netuid)
        validator_permit = bool(metagraph.validator_permit[uid])
        stake = int(metagraph.alpha_stake[uid]) > 10_000
        if validator_permit and stake:
            return True
        
        return False
    except Exception as e:
        print(f"Error verifying validator status: {str(e)}")
        return False


def verify_commitment(
        hotkey: str,
        expected_prefix: str,
        netuid: int,
        network: str,
        max_age_seconds: int = 60
) -> bool:
    """
    Verify that a commitment exists and matches the expected format, within the time window

    Args:
        hotkey: The hotkey to check
        expected_prefix: The expected prefix of the commitment
        netuid: The subnet ID
        network: The network name
        max_age_seconds: Maximum age of commitment in seconds

    Returns:
        bool: True if commitment is valid and recent
    """
    # Get commitment from chain

    commitment = get_commitment(hotkey, netuid, network)
    if not commitment:
        return False

    try:
        # Check if it starts with expected prefix
        if not commitment.startswith(expected_prefix):
            return False

        # For validator, additionally verify validator status
        if "validator" in expected_prefix and not verify_validator_status(hotkey, netuid, network):
            return False

        # Check if commitment timestamp is within allowed window
        # Format should end with timestamp
        parts = commitment.split(":")
        if len(parts) < 2:
            return False

        # Get timestamp from last part
        try:
            timestamp = int(parts[-1])
            current_time = int(time.time())

            # Check if commitment is not too old
            if (current_time - timestamp) > max_age_seconds:
                print(f"Commitment too old: {current_time - timestamp} seconds")
                return False

            return True
        except ValueError:
            print(f"Invalid timestamp in commitment: {parts[-1]}")
            return False

    except Exception as e:
        print(f"Error validating commitment: {str(e)}")
        return False