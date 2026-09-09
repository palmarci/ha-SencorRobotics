#!/usr/bin/env python3
"""Interactive CLI shell for Sencor robot vacuums (Clouds Robot protocol).

Protocol: TCP port 8888, binary header + JSON body.
Header: 20 bytes, first 4 bytes little-endian length of (header + body).
Body:   {"cmd":0,"control":{"authCode":..,"deviceIp":..,"devicePort":"8888",
          "targetId":..,"targetType":"3"},"seq":0,"value":{..},"version":..}

Works standalone. No third-party dependencies.
"""
import argparse
import asyncio
import base64
import cmd
import json
import os
import sys
from typing import Any

PORT = 8888
DEFAULT_VERSION = "1.5.11"
VERSION_CLEAN_ROOMS = "2.3.1"
CONFIG_PATH = os.path.expanduser("~/.config/sencor.json")

WORK_MODE = {"silent": 9, "standard": 1, "intensive": 7}
# workMode values observed on firmware 3.12.1720(500): 0=off, 1=standard,
# 7=intensive, 9=silent, 10/11=standard variants. Robot may use others.
MODE_NAMES = {0: "off", 1: "standard", 7: "intensive", 9: "silent",
              10: "standard", 11: "standard"}
FAN_NAMES = {0: "off", 1: "low", 2: "standard", 3: "high"}
WORK_STATE = {
    1: "cleaning", 2: "idle", 4: "returning", 5: "charging",
    6: "docked", 7: "error", 9: "returning", 10: "docked",
}
MOP_MODE = {"high": 20, "medium": 40, "low": 60}
ERROR_CODES = {
    3: "power switch not switched on during charging",
    13: "right wheel suspended",
    14: "left wheel suspended",
    104: "side brush stuck",
    105: "rear wheel overload",
    106: "stuck",
    109: "no contact with floor",
    119: "localization failed",
}


def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as fh:
            return json.load(fh)
    return {}


def save_config(cfg):
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, "w") as fh:
        json.dump(cfg, fh, indent=2)


def parse_value(value: Any) -> Any:
    """Parse JSON-ish response values: nested json, ; and , lists, ints."""
    if isinstance(value, str):
        try:
            return parse_value(json.loads(value))
        except Exception:
            pass
        parts = value.split(";")
        if len(parts) > 1:
            return list(map(parse_value, parts))
        parts = value.split(",")
        if len(parts) > 1:
            return list(map(parse_value, parts))
        if value.replace(".", "", 1).isdigit():
            return int(value)
    if isinstance(value, dict):
        return {k: parse_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return list(map(parse_value, value))
    return value


class Vacuum:
    """Connection to a Sencor vacuum over the Clouds Robot protocol."""

    def __init__(self, host, auth_code, device_id):
        self.host = host
        self.auth_code = auth_code
        self.device_id = device_id
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

    async def _connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, PORT)
        setattr(self.writer, "write_timeout", 10)
        setattr(self.writer, "read_timeout", 10)

    async def _close(self):
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass
        self.reader = None
        self.writer = None

    @staticmethod
    def _size_prefix(size):
        """Little-endian hex of the 32-bit size, byte-swapped pairs."""
        size_hex = "{0:08x}".format(size)
        return "".join(map(str.__add__, size_hex[-2::-2], size_hex[-1::-2]))

    def _build_packet(self, data, version):
        datastring = json.dumps(data, separators=(",", ":"))
        body = (
            '{"cmd":0,"control":{"authCode":"' + self.auth_code +
            '","deviceIp":"' + self.host +
            '","devicePort":"8888","targetId":"' + self.device_id +
            '","targetType":"3"},"seq":0,"value":' + datastring +
            ',"version":"' + version + '"}'
        ).encode()
        request_size = len(body) + 20
        prefix = self._size_prefix(request_size)
        header = bytes.fromhex(f"{prefix}fa00c8000000eb27ea27000000000000")
        return header + body

    async def _request(self, data, version=DEFAULT_VERSION, timeout=15):
        await self._connect()
        writer = self.writer
        assert writer is not None
        try:
            writer.write(self._build_packet(data, version))
            await writer.drain()
            return await asyncio.wait_for(self._read_response(), timeout)
        finally:
            await self._close()

    async def _read_response(self):
        reader = self.reader
        assert reader is not None
        header = await reader.readexactly(20)
        raw_size_hex = header[:4].hex()
        size_hex = "".join(
            map(str.__add__, raw_size_hex[-2::-2], raw_size_hex[-1::-2])
        )
        size = int(size_hex, 16) - len(header)
        data = b""
        while len(data) < size:
            chunk = await reader.readexactly(size - len(data))
            data += chunk
        return parse_value(data.decode("ascii"))

    async def get_state(self):
        return (await self._request({"transitCmd": "98"}))["value"]

    async def get_map(self):
        data = {
            "mapWidth": "0", "centerPoint": "0", "mapHeight": "0",
            "trackNum": "AAA=", "mapSign": "AAA=", "transitCmd": "133",
        }
        return (await self._request(data, timeout=6))["value"]

    async def start(self, mode=None):
        if mode is None:
            data = {"start": "1", "transitCmd": "100"}
        else:
            data = {"mode": str(WORK_MODE[mode]), "transitCmd": "106"}
        await self._request(data)

    async def stop(self):
        await self._request({"stop": "1", "isStop": "1", "transitCmd": "102"})

    async def pause(self):
        await self._request({"pause": "1", "isStop": "0", "transitCmd": "102"})

    async def dock(self):
        await self._request({"charge": "1", "transitCmd": "104"})

    async def find(self):
        await self._request({"find": "", "transitCmd": "143"})

    async def set_mop_mode(self, mode):
        await self._request({"waterTank": str(MOP_MODE[mode]), "transitCmd": "145"})

    async def set_volume(self, volume):
        vol = 1 + round((volume / 100) * 10) / 10
        await self._request({"volume": str(vol), "voice": "", "transitCmd": "123"})

    async def clean_rooms(self, rooms):
        cleaned = []
        for room in rooms:
            room_id = str(room["room_id"])
            if not any(room_id == x["blockNum"] for x in cleaned):
                cleaned.append({
                    "cleanNum": str(room.get("clean_num", 1)),
                    "blockNum": room_id,
                })
        data = {"opCmd": "cleanBlocks", "cleanBlocks": sorted(cleaned, key=lambda x: x["blockNum"])}
        await self._request(data, VERSION_CLEAN_ROOMS)


class SencorShell(cmd.Cmd):
    intro = "Sencor vacuum CLI. Type 'help' for commands."
    prompt = "sencor> "

    def __init__(self):
        super().__init__()
        cfg = load_config()
        self.vac = None
        if cfg.get("host") and cfg.get("auth_code") and cfg.get("device_id"):
            self.vac = Vacuum(cfg["host"], cfg["auth_code"], cfg["device_id"])
            self.prompt = f"sencor[{cfg['host']}]> "

    def _require_vac(self) -> Vacuum | None:
        if not self.vac:
            print("No device configured. Run 'connect <host> <auth_code> <device_id>'.")
            return None
        return self.vac

    def _run(self, coro) -> Any:
        try:
            return asyncio.run(coro)
        except TimeoutError:
            print("No response from device (timed out).")
            return None
        except (ConnectionError, OSError) as exc:
            print(f"Connection failed: {exc}")
            return None

    def do_connect(self, arg):
        """connect <host> <auth_code> <device_id>  -- set device and save config"""
        parts = arg.split()
        if len(parts) != 3:
            print("Usage: connect <host> <auth_code> <device_id>")
            return
        host, auth_code, device_id = parts
        self.vac = Vacuum(host, auth_code, device_id)
        save_config({"host": host, "auth_code": auth_code, "device_id": device_id})
        self.prompt = f"sencor[{host}]> "
        print(f"Connected device set. Test with 'status'.")

    def do_status(self, arg):
        """status  -- battery, work state, mode, fan, position"""
        vac = self._require_vac()
        if not vac:
            return
        state = self._run(vac.get_state())
        if state is None:
            return
        battery = state.get("battery")
        work_state = WORK_STATE.get(state.get("workState"), "unknown")
        work_mode = MODE_NAMES.get(state.get("workMode"), f"mode {state.get('workMode')}")
        fan = FAN_NAMES.get(state.get("fan"), state.get("fan"))
        water_tank = state.get("waterTank", 0)
        error = state.get("error")
        print(f"battery:    {battery}%")
        print(f"work state: {work_state}")
        print(f"work mode:  {work_mode}")
        print(f"fan:        {fan}")
        if water_tank:
            print(f"mop flow:   {water_tank} (robot does not report tank level)")
        print(f"volume:     {state.get('voice', 0)}")
        print(f"version:    {state.get('version', '?')}")
        if error:
            err = ERROR_CODES.get(error, f"code {error}")
            print(f"error:      {err}")
        if state.get("extParam"):
            print(f"ext params: {state['extParam']}")

    def do_record(self, arg):
        """record  -- last clean record (time, area, map sign)"""
        vac = self._require_vac()
        if not vac:
            return
        rec = self._run(vac._request({"transitCmd": "131"}))
        if rec is None:
            return
        val = rec.get("value", rec) if isinstance(rec, dict) else rec
        print(f"clean time: {val.get('clearTime', '?')} min")
        print(f"clean area: {val.get('clearArea', '?')} m2")
        print(f"sign:       {val.get('clearSign', '?')}")
        print(f"charger:    {val.get('chargerPos', '?')}")
        print(f"map data:   {'yes' if val.get('map') else 'no'}")

    def do_rooms(self, arg):
        """rooms  -- list known rooms (needs a saved map)"""
        vac = self._require_vac()
        if not vac:
            return
        try:
            map_value = self._run(vac.get_map())
        except Exception:
            map_value = None
        if map_value is None:
            print("No map response. The robot has no saved map yet.")
            print("Run a full cleaning first so the robot builds its map.")
            return
        found = False
        if "regionNames" in map_value:
            for room in map_value["regionNames"]:
                found = True
                try:
                    name = base64.b64decode(room["regionName"]).decode("utf-8")
                except Exception:
                    name = room["regionName"]
                print(f"{room['regionNum']}: {name}")
        if "chargerPos" in map_value:
            print(f"charger pos: {map_value['chargerPos']}")
        if "robotPos" in map_value:
            print(f"robot pos:   {map_value['robotPos']}")
        if not found:
            print("No regions in map data. The robot has no saved map yet.")

    def do_start(self, arg):
        """start [silent|standard|intensive]  -- start cleaning (full or with fan mode)"""
        vac = self._require_vac()
        if not vac:
            return
        mode = arg.strip() or None
        if mode and mode not in WORK_MODE:
            print(f"Unknown mode '{mode}'. Use: {', '.join(WORK_MODE)}")
            return
        self._run(vac.start(mode))
        print("Started.")

    def do_stop(self, arg):
        """stop  -- stop cleaning"""
        vac = self._require_vac()
        if not vac:
            return
        self._run(vac.stop())
        print("Stopped.")

    def do_pause(self, arg):
        """pause  -- pause cleaning"""
        vac = self._require_vac()
        if not vac:
            return
        self._run(vac.pause())
        print("Paused.")

    def do_dock(self, arg):
        """dock  -- return to charging dock"""
        vac = self._require_vac()
        if not vac:
            return
        self._run(vac.dock())
        print("Returning to dock.")

    def do_find(self, arg):
        """find  -- locate the robot (beeps)"""
        vac = self._require_vac()
        if not vac:
            return
        self._run(vac.find())
        print("Locating robot.")

    def do_mop(self, arg):
        """mop <high|medium|low>  -- set water flow (no level readback; dispenses only while mopping)"""
        vac = self._require_vac()
        if not vac:
            return
        mode = arg.strip().lower()
        if mode not in MOP_MODE:
            print(f"Usage: mop <{'|'.join(MOP_MODE)}>")
            return
        print("Note: the robot does not report the water level back.")
        print("      Water dispenses only while a mopping job runs.")
        print("      When stopped, only gravity residue drips.")
        self._run(vac.set_mop_mode(mode))
        print(f"Water flow set to {mode}.")

    def do_volume(self, arg):
        """volume <0-100>  -- set voice volume"""
        vac = self._require_vac()
        if not vac:
            return
        try:
            volume = int(arg)
            if not 0 <= volume <= 100:
                raise ValueError
        except ValueError:
            print("Usage: volume <0-100>")
            return
        self._run(vac.set_volume(volume))
        print(f"Volume set to {volume}.")

    def do_clean(self, arg):
        """clean <room_id> [room_id ...] [x<times>]  -- clean specific rooms"""
        vac = self._require_vac()
        if not vac:
            return
        parts = arg.split()
        if not parts:
            print("Usage: clean <room_id> [room_id ...] [x<times>]")
            return
        times = 1
        room_ids = []
        for part in parts:
            if part.startswith("x") and part[1:].isdigit():
                times = int(part[1:])
            elif part.isdigit():
                room_ids.append(int(part))
            else:
                print(f"Invalid room id '{part}'.")
                return
        rooms = [{"room_id": rid, "clean_num": times} for rid in room_ids]
        self._run(vac.clean_rooms(rooms))
        print(f"Cleaning rooms {room_ids} (x{times}).")

    def do_raw(self, arg):
        """raw <transitCmd> <json>  -- send raw command, print response"""
        vac = self._require_vac()
        if not vac:
            return
        parts = arg.split(None, 1)
        if len(parts) != 2:
            print("Usage: raw <transitCmd> <json>")
            return
        transit_cmd, json_str = parts
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as exc:
            print(f"Invalid JSON: {exc}")
            return
        data["transitCmd"] = transit_cmd
        response = self._run(vac._request(data))
        print(json.dumps(response, indent=2, default=str))

    def do_exit(self, arg):
        """exit  -- leave the shell"""
        print("Bye.")
        return True

    def do_quit(self, arg):
        """quit  -- leave the shell"""
        return self.do_exit(arg)

    def do_EOF(self, arg):
        print()
        return True


def main():
    parser = argparse.ArgumentParser(description="Sencor robot vacuum CLI")
    parser.add_argument("--host", help="vacuum IP address")
    parser.add_argument("--auth-code", help="auth code from Sencor app")
    parser.add_argument("--device-id", help="device id from Sencor app")
    parser.add_argument("-c", "--command", help="run one command and exit, e.g. 'status'")
    args = parser.parse_args()

    cfg = load_config()
    if args.host:
        cfg["host"] = args.host
    if args.auth_code:
        cfg["auth_code"] = args.auth_code
    if args.device_id:
        cfg["device_id"] = args.device_id

    if args.command:
        shell = SencorShell()
        if args.host or args.auth_code or args.device_id:
            if cfg.get("host") and cfg.get("auth_code") and cfg.get("device_id"):
                shell.vac = Vacuum(cfg["host"], cfg["auth_code"], cfg["device_id"])
                save_config(cfg)
        shell.onecmd(args.command)
        return

    SencorShell().cmdloop()


if __name__ == "__main__":
    main()