"""Coordinator for Hive Local Thermostat integration."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

from homeassistant.components.climate.const import (
    PRESET_BOOST,
    PRESET_NONE,
    HVACAction,
    HVACMode,
)
from homeassistant.components.mqtt import client as mqtt_client
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util.dt import utcnow

from .const import (
    DEFAULT_FROST_TEMPERATURE,
    DEFAULT_HEATING_BOOST_MINUTES,
    DEFAULT_HEATING_BOOST_TEMPERATURE,
    DEFAULT_WATER_BOOST_MINUTES,
    DOMAIN,
    FROST_PROTECTION_SETPOINT,
    HIVE_BOOST,
    LOGGER,
    MODEL_SLR2,
)

PRESET_MAP = {
    PRESET_NONE: "",
    PRESET_BOOST: HIVE_BOOST,
}

BOOST_ERROR = 65000


@dataclass(frozen=True)
class ZoneKeys:
    """Zigbee2MQTT attribute names for one heating/water zone."""

    mode: str
    hold: str
    hold_duration: str
    setpoint: str = ""  # unused for water, which has no setpoint attribute


# SLR1/OTR1 expose a single heating zone with unsuffixed attribute names
HEAT_KEYS_SINGLE = ZoneKeys(
    mode="system_mode",
    hold="temperature_setpoint_hold",
    hold_duration="temperature_setpoint_hold_duration",
    setpoint="occupied_heating_setpoint",
)
# SLR2 suffixes its attributes per zone
HEAT_KEYS_SLR2 = ZoneKeys(
    mode="system_mode_heat",
    hold="temperature_setpoint_hold_heat",
    hold_duration="temperature_setpoint_hold_duration_heat",
    setpoint="occupied_heating_setpoint_heat",
)
WATER_KEYS_SLR2 = ZoneKeys(
    mode="system_mode_water",
    hold="temperature_setpoint_hold_water",
    hold_duration="temperature_setpoint_hold_duration_water",
)


class HiveCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Class to manage fetching Hive data from MQTT."""

    current_temperature: float | None = None
    target_temperature: float | None = None
    preset_mode: str | None = None
    hvac_mode: HVACMode | None = None
    water_mode: str | None = None

    running_state_heat: str = ""
    running_state_water: str = ""

    heat_boost: bool = False
    water_boost: bool = False
    heat_boost_started: datetime | None = None
    water_boost_started: datetime | None = None
    heat_boost_started_duration: int = 0
    water_boost_started_duration: int = 0
    heat_boost_remaining: int = 0
    water_boost_remaining: int = 0

    pre_boost_hvac_mode: HVACMode | None = None
    pre_boost_occupied_heating_setpoint_heat: float | None = None
    pre_boost_water_mode: str | None = None

    # Number entity values
    heating_boost_duration: float = DEFAULT_HEATING_BOOST_MINUTES
    heating_boost_temperature: float = DEFAULT_HEATING_BOOST_TEMPERATURE
    heating_frost_prevention: float = DEFAULT_FROST_TEMPERATURE
    water_boost_duration: float = DEFAULT_WATER_BOOST_MINUTES

    # SLR2 pretend-off state (frost protection sent, but HA shows OFF)
    _user_set_off: bool = False
    # Temperature to restore when turning back on after a user-initiated OFF
    _pre_off_temperature: float | None = None
    # Last setpoint received with hold=False (genuine schedule target), used to suppress
    # stale hold=True echoes during SLR2 schedule-period transitions.
    _last_schedule_setpoint: float | None = None

    # Diagnostics
    last_mqtt_payload: dict[str, Any] | None = None

    def __init__(
        self,
        hass: HomeAssistant,
        entry_id: str,
        model: str,
        topic: str,
        show_heat_schedule_mode: bool,  # noqa: FBT001
        show_water_schedule_mode: bool,  # noqa: FBT001
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            LOGGER,
            name=f"{DOMAIN}_{entry_id}",
        )
        self.entry_id = entry_id
        self.model = model
        self.heat_keys = HEAT_KEYS_SLR2 if model == MODEL_SLR2 else HEAT_KEYS_SINGLE
        self.water_keys = WATER_KEYS_SLR2
        self.topic = topic
        self.show_heating_schedule_mode = show_heat_schedule_mode
        self.show_water_schedule_mode = show_water_schedule_mode
        self.data: dict[str, Any] = {}

    @property
    def topic_get(self) -> str:
        """Return the topic getter."""
        return self.topic + "/get"

    @property
    def topic_set(self) -> str:
        """Return the topic setter."""
        return self.topic + "/set"

    @property
    def boost_remaining_heat(self) -> int:
        """Return the remaining boost time for heating."""
        return self.heat_boost_remaining

    @property
    def boost_remaining_water(self) -> int:
        """Return the remaining boost time for water."""
        return self.water_boost_remaining

    @property
    def hvac_action(self) -> HVACAction | None:
        """Return the current HVAC action."""
        if self.hvac_mode == HVACMode.OFF:
            return HVACAction.OFF
        if self.running_state_heat == "preheating":
            return HVACAction.PREHEATING
        if self.running_state_heat == "heat":
            return HVACAction.HEATING
        if self.running_state_heat == "idle":
            return HVACAction.IDLE
        if self.running_state_heat == "off":
            return HVACAction.OFF
        return None

    @property
    def local_temperature_heat(self) -> float | None:
        """Return the local temperature for heating."""
        return self.current_temperature

    @property
    def pre_off_temperature(self) -> float | None:
        """Return the stored pre-off setpoint, if any."""
        return self._pre_off_temperature

    def climate_preset(self, mode: str) -> str:
        """Get the current preset."""
        return next(
            (k for k, v in PRESET_MAP.items() if v == mode), PRESET_MAP[PRESET_NONE]
        )

    @callback
    def handle_mqtt_message(self, message: ReceiveMessage) -> None:  # noqa: C901, PLR0912, PLR0915
        """Handle received MQTT message."""
        topic = message.topic
        payload = message.payload
        LOGGER.debug("Received from %s payload: %s", topic, payload)

        if not payload:
            LOGGER.error(
                "Received empty payload on topic %s, check that you have the correct topic name",
                topic,
            )
            return

        self.current_temperature = None
        self.target_temperature = None
        self.preset_mode = None
        self.hvac_mode = None
        self.heat_boost = False
        self.water_boost = False
        self.water_mode = None

        try:
            parsed_data: dict[str, Any] = json.loads(payload)

            # Store last payload for diagnostics
            self.last_mqtt_payload = parsed_data

            if not self.valid_data_for_model(parsed_data):
                return

            if self.model == MODEL_SLR2:
                reported_boost_remaining_heat = cast(
                    int,
                    parsed_data["temperature_setpoint_hold_duration_heat"]
                    if parsed_data["system_mode_heat"] == "emergency_heating"
                    else 0,
                )
                reported_boost_remaining_water = cast(
                    int,
                    parsed_data["temperature_setpoint_hold_duration_water"]
                    if parsed_data["system_mode_water"] == "emergency_heating"
                    else 0,
                )
                reported_boost_temperature = parsed_data[
                    "occupied_heating_setpoint_heat"
                ]
                self.running_state_heat = cast(
                    str,
                    parsed_data.get("running_state_heat")
                    if parsed_data.get("running_state_heat")
                    else "preheating",
                )
                self.running_state_water = cast(
                    str,
                    parsed_data.get("running_state_water")
                    if parsed_data.get("running_state_water")
                    else "preheating",
                )
                self.current_temperature = parsed_data["local_temperature_heat"]
                if parsed_data["occupied_heating_setpoint_heat"] == 1:
                    self.target_temperature = self.heating_frost_prevention
                else:
                    self.target_temperature = parsed_data[
                        "occupied_heating_setpoint_heat"
                    ]
                # When user-set OFF, the device reports the frost-protection setpoint.
                # Restore the pre-off temperature so HA displays and resumes correctly.
                if self._user_set_off and self._pre_off_temperature is not None:
                    self.target_temperature = self._pre_off_temperature

                # Guard against SLR2 schedule-transition race condition.
                # When the SLR2 transitions schedule periods it emits a burst of MQTT
                # messages. The genuine new setpoint arrives with hold=False; stale
                # echoes of the prior period's setpoint arrive with hold=True.
                # Strategy: track the last hold=False setpoint as the authoritative
                # target, and suppress any hold=True message that differs from it.
                # This catches echoes at any temperature (7°C frost, 19°C prior
                # setpoint, etc.) not just frost-level values.
                # Skip suppression when the user intentionally set OFF via HA —
                # that command also uses hold=True and must not be overridden.
                if parsed_data.get("temperature_setpoint_hold_heat") is False:
                    # Genuine schedule setpoint — record it as the authoritative target.
                    self._last_schedule_setpoint = self.target_temperature
                elif (
                    not self._user_set_off
                    and parsed_data.get("temperature_setpoint_hold_heat") is True
                    and self._last_schedule_setpoint is not None
                    and self.target_temperature != self._last_schedule_setpoint
                ):
                    LOGGER.warning(
                        "Suppressing stale setpoint echo during schedule transition "
                        "(ignoring %.1f°C, retaining %.1f°C)",
                        self.target_temperature,
                        self._last_schedule_setpoint,
                    )
                    self.target_temperature = self._last_schedule_setpoint

                self.preset_mode = self.climate_preset(parsed_data["system_mode_heat"])
                if parsed_data["system_mode_heat"] == "auto":
                    self.hvac_mode = HVACMode.AUTO
                if parsed_data["system_mode_heat"] == "heat":
                    if (
                        parsed_data["temperature_setpoint_hold_heat"] is False
                        and self.show_heating_schedule_mode
                    ):
                        self.hvac_mode = HVACMode.AUTO
                    else:
                        self.hvac_mode = HVACMode.HEAT
                if parsed_data["system_mode_heat"] == "emergency_heating":
                    self.hvac_mode = HVACMode.HEAT
                    self.heat_boost = True
                if parsed_data["system_mode_heat"] == "off":
                    self.hvac_mode = HVACMode.OFF
                if self._user_set_off:
                    self.hvac_mode = HVACMode.OFF

                if (
                    parsed_data["system_mode_heat"] != "emergency_heating"
                    and self.hvac_mode != HVACMode.OFF
                ):
                    self.pre_boost_occupied_heating_setpoint_heat = (
                        self.target_temperature
                    )
                    self.pre_boost_hvac_mode = self.hvac_mode
                if parsed_data["system_mode_water"] == "auto":
                    self.water_mode = "auto"
                if parsed_data["system_mode_water"] == "heat":
                    if parsed_data["temperature_setpoint_hold_water"] is False:
                        if self.show_water_schedule_mode:
                            self.water_mode = "auto"
                        else:
                            self.water_mode = "heat"
                    else:
                        self.water_mode = "heat"
                if parsed_data["system_mode_water"] == "emergency_heating":
                    self.water_mode = "boost"
                    self.water_boost = True
                if parsed_data["system_mode_water"] == "off":
                    self.water_mode = "off"

                if parsed_data["system_mode_water"] != "emergency_heating":
                    self.pre_boost_water_mode = self.water_mode
            else:
                reported_boost_remaining_heat = cast(
                    int,
                    parsed_data["temperature_setpoint_hold_duration"]
                    if parsed_data["system_mode"] == "emergency_heating"
                    else 0,
                )
                reported_boost_temperature = parsed_data["occupied_heating_setpoint"]
                self.running_state_heat = cast(
                    str,
                    parsed_data.get("running_state")
                    if parsed_data.get("running_state")
                    else "preheating",
                )
                self.current_temperature = parsed_data["local_temperature"]
                if parsed_data["occupied_heating_setpoint"] == 1:
                    self.target_temperature = self.heating_frost_prevention
                else:
                    self.target_temperature = parsed_data["occupied_heating_setpoint"]
                self.preset_mode = self.climate_preset(parsed_data["system_mode"])

                if parsed_data["system_mode"] == "auto":
                    self.hvac_mode = HVACMode.AUTO
                if parsed_data["system_mode"] == "heat":
                    if (
                        parsed_data["temperature_setpoint_hold"] is False
                        and self.show_heating_schedule_mode
                    ):
                        self.hvac_mode = HVACMode.AUTO
                    else:
                        self.hvac_mode = HVACMode.HEAT
                if parsed_data["system_mode"] == "emergency_heating":
                    self.hvac_mode = HVACMode.HEAT
                    self.heat_boost = True
                if parsed_data["system_mode"] == "off":
                    self.hvac_mode = HVACMode.OFF

                if parsed_data["system_mode"] != "emergency_heating":
                    self.pre_boost_occupied_heating_setpoint_heat = (
                        self.target_temperature
                    )
                    self.pre_boost_hvac_mode = self.hvac_mode

            if self.correct_heat_boost(
                reported_boost_remaining_heat, reported_boost_temperature
            ):
                return  # Correction made, exit to avoid state update loop
            self.record_heat_boost_state()

            if self.model == MODEL_SLR2:
                if self.correct_water_boost(reported_boost_remaining_water):
                    return  # Correction made, exit to avoid state update loop
                self.record_water_boost_state()

            self.async_set_updated_data(parsed_data)
        except json.JSONDecodeError:
            LOGGER.error("Failed to parse JSON from MQTT payload: %s", payload)
        except Exception as err:  # noqa: BLE001
            LOGGER.error("Error handling MQTT message: %s", err)

    def valid_data_for_model(self, data: dict[str, Any]) -> bool:
        """Check if data is valid for the current model."""
        if self.model == MODEL_SLR2:
            if "system_mode_water" not in data:
                LOGGER.error(
                    "Received data does not contain 'system_mode_water' for SLR2, check you have the correct model set"
                )
                return False
        elif "system_mode_water" in data:
            LOGGER.error(
                "Received data contains 'system_mode_water' for SLR1/OTR1, check you have the correct model set"
            )
            return False
        return True

    def correct_heat_boost(
        self, reported_boost_remaining_heat: int, reported_boost_temperature: float
    ) -> bool:
        """Check and correct boost remaining heat if necessary."""
        if reported_boost_remaining_heat > BOOST_ERROR:
            # Calculate remaining boost time based on when it started
            tracked = bool(
                self.heat_boost_started and self.heat_boost_started_duration > 0
            )
            if tracked and self.heat_boost_started:
                elapsed = (utcnow() - self.heat_boost_started).total_seconds() / 60
                self.heat_boost_remaining = max(
                    0, int(self.heat_boost_started_duration - elapsed)
                )
            else:
                self.heat_boost_remaining = 0

            LOGGER.warning(
                "Correcting reported boost remaining heat from %d to %d",
                reported_boost_remaining_heat,
                self.heat_boost_remaining,
            )
            if self.config_entry is not None:
                if tracked and self.heat_boost_remaining == 0:
                    # Boost has run its course; re-issuing it would extend it
                    self.config_entry.async_create_task(
                        self.hass, self.async_heating_boost_cancel()
                    )
                else:
                    # Unknown remaining time (e.g. restart mid-boost) falls back
                    # to the configured duration
                    self.config_entry.async_create_task(
                        self.hass,
                        self.async_heating_boost(
                            self.heat_boost_remaining or None,
                            reported_boost_temperature,
                        ),
                    )
            return True
        self.heat_boost_remaining = reported_boost_remaining_heat
        return False

    def correct_water_boost(self, reported_boost_remaining_water: int) -> bool:
        """Check and correct boost remaining water if necessary."""
        if reported_boost_remaining_water > BOOST_ERROR:
            # Calculate remaining boost time based on when it started
            tracked = bool(
                self.water_boost_started and self.water_boost_started_duration > 0
            )
            if tracked and self.water_boost_started:
                elapsed = (utcnow() - self.water_boost_started).total_seconds() / 60
                self.water_boost_remaining = max(
                    0, int(self.water_boost_started_duration - elapsed)
                )
            else:
                self.water_boost_remaining = 0

            LOGGER.warning(
                "Correcting reported boost remaining water from %d to %d",
                reported_boost_remaining_water,
                self.water_boost_remaining,
            )
            if self.config_entry is not None:
                if tracked and self.water_boost_remaining == 0:
                    self.config_entry.async_create_task(
                        self.hass, self.async_water_boost_cancel()
                    )
                else:
                    self.config_entry.async_create_task(
                        self.hass,
                        self.async_water_boost(self.water_boost_remaining or None),
                    )
            return True
        self.water_boost_remaining = reported_boost_remaining_water
        return False

    def record_heat_boost_state(self) -> None:
        """Record and track boost state for heating."""
        if self.heat_boost and self.heat_boost_remaining > 0:
            # Boost is active, record the start time if not already recorded
            if not self.heat_boost_started:
                self.heat_boost_started = utcnow()
                self.heat_boost_started_duration = self.heat_boost_remaining
        elif not self.heat_boost:
            # Boost is not active, clear tracking state
            self.heat_boost_started = None
            self.heat_boost_started_duration = 0

        if self.water_boost and self.water_boost_remaining > 0:
            # Water boost is active, record the start time if not already recorded
            if not self.water_boost_started:
                self.water_boost_started = utcnow()
                self.water_boost_started_duration = self.water_boost_remaining
        elif not self.water_boost:
            # Water boost is not active, clear tracking state
            self.water_boost_started = None
            self.water_boost_started_duration = 0

    def record_water_boost_state(self) -> None:
        """Record and track boost state for water."""
        if self.water_boost and self.water_boost_remaining > 0:
            # Water boost is active, record the start time if not already recorded
            if not self.water_boost_started:
                self.water_boost_started = utcnow()
                self.water_boost_started_duration = self.water_boost_remaining
        elif not self.water_boost:
            # Water boost is not active, clear tracking state
            self.water_boost_started = None
            self.water_boost_started_duration = 0

    async def _async_publish_set(self, payload: dict[str, Any]) -> None:
        """Publish MQTT set message."""
        message = json.dumps(payload)
        LOGGER.debug("Sending to %s message %s", self.topic_set, message)
        await mqtt_client.async_publish(self.hass, self.topic_set, message)

    async def async_water_boost(
        self, boost_duration_minutes: int | None = None
    ) -> None:
        """Send water boost command."""

        if not self.water_boost:
            self.pre_boost_water_mode = self.water_mode

        duration = int(boost_duration_minutes or self.water_boost_duration)
        k = self.water_keys

        self.water_boost = True
        self.water_boost_started = utcnow()
        self.water_boost_started_duration = duration

        await self._async_publish_set(
            {k.mode: "emergency_heating", k.hold_duration: duration, k.hold: 1}
        )

    async def async_water_boost_cancel(self) -> None:
        """Cancel water boost command."""

        if self.pre_boost_water_mode == "auto":
            await self.async_water_scheduled()
        elif self.pre_boost_water_mode == "heat":
            await self.async_water_always_on()
        else:
            await self.async_water_always_off()

    async def async_water_scheduled(self) -> None:
        """Send water scheduled command."""

        await self._async_publish_set({self.water_keys.mode: "auto"})

    async def async_water_always_on(self) -> None:
        """Send water always on command."""

        k = self.water_keys
        await self._async_publish_set({k.mode: "heat", k.hold: 1})

    async def async_water_always_off(self) -> None:
        """Send water always off command."""

        k = self.water_keys
        await self._async_publish_set({k.mode: "off", k.hold: 0})

    async def async_heating_boost(
        self,
        boost_duration_minutes: int | None = None,
        boost_temperature: float | None = None,
    ) -> None:
        """Send heating boost command."""

        # Only capture state when not already boosting, otherwise (extending or
        # correcting a boost) we would overwrite it with the boosted values
        if not self.heat_boost:
            self.pre_boost_occupied_heating_setpoint_heat = self.target_temperature
            self.pre_boost_hvac_mode = self.hvac_mode

        duration = int(boost_duration_minutes or self.heating_boost_duration)
        temperature = boost_temperature or self.heating_boost_temperature
        k = self.heat_keys

        self.heat_boost = True
        self.heat_boost_started = utcnow()
        self.heat_boost_started_duration = duration

        await self._async_publish_set(
            {
                k.mode: "emergency_heating",
                k.hold_duration: duration,
                k.hold: 1,
                k.setpoint: temperature,
            }
        )

    async def async_heating_boost_cancel(self) -> None:
        """Cancel heating boost command."""

        k = self.heat_keys

        # Cancelling a boost in the SLR1 while the system is in manual mode actually reverts to the schedule.
        # This basically maintains that behaviour for consistency. I cannot verify what the SLR2 does.
        mode = "heat"
        if (
            self.pre_boost_hvac_mode not in (HVACMode.AUTO, HVACMode.HEAT)
            and self.model == MODEL_SLR2
        ):
            mode = "off"

        await self._async_publish_set({k.mode: mode, k.hold: 0})

    async def async_set_temperature(self, temperature: float) -> None:
        """Set temperature."""

        # For SLR2 while user-set OFF, the device is in frost-protection HEAT mode.
        # Sending a setpoint would cause it to start heating immediately, so only
        # store the temperature for resume and do not publish to the device.
        if self._user_set_off and self.model == MODEL_SLR2:
            self._pre_off_temperature = temperature
            return

        # If the user changes the setpoint while off (SLR1/OTR1), track it as the new resume temperature
        if self._user_set_off:
            self._pre_off_temperature = temperature

        await self._async_publish_set({self.heat_keys.setpoint: temperature})

    async def async_set_hvac_mode_off(self) -> None:
        """Set HVAC mode to off."""

        k = self.heat_keys
        payload: dict[str, Any]
        if self.model == MODEL_SLR2:
            # SLR2 has no usable "off" for heating; hold frost protection instead
            payload = {
                k.mode: "heat",
                k.hold: True,
                k.setpoint: FROST_PROTECTION_SETPOINT,
            }
        else:
            payload = {k.mode: "off", k.hold: False}

        # Save the current setpoint so it can be restored when turning back on
        if self.target_temperature is not None:
            self._pre_off_temperature = self.target_temperature
        self._user_set_off = True
        self.hvac_mode = HVACMode.OFF
        await self._async_publish_set(payload)

    async def async_set_hvac_mode_auto(self) -> None:
        """Set HVAC mode to auto."""

        self._user_set_off = False
        self.hvac_mode = HVACMode.AUTO
        await self._async_publish_set({self.heat_keys.mode: "auto"})

    async def async_set_hvac_mode_heat(
        self,
        temperature: float,
        set_from_temperature: bool = False,  # noqa: ARG002, FBT001, FBT002
    ) -> None:
        """Set HVAC mode to heat."""

        k = self.heat_keys
        self._user_set_off = False
        self._pre_off_temperature = None
        self.hvac_mode = HVACMode.HEAT
        await self._async_publish_set(
            {
                k.mode: "heat",
                k.setpoint: temperature,
                k.hold: True,
                k.hold_duration: 0,
            }
        )
