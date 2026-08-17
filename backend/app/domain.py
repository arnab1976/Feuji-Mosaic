"""
MOSAIC — domain model.

The five monitored SCADA parameters, their control/alarm/trip bands, the
asset they belong to, the model that would score them, and the independent
variables (drivers) that explain a breach.

This is the single source of truth reused across every layer.
"""
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import List, Tuple, Dict


@dataclass
class Parameter:
    id: str
    name: str
    short: str
    unit: str
    tag: str                       # the raw SCADA tag (the primary key)
    asset: str
    control: Tuple[float, float]   # normal band
    alarm: Tuple[float, float]     # agent acts
    trip: Tuple[float, float]      # QA e-signature required
    model: str                     # model that would score it (context, not run here)
    drivers: List[str] = field(default_factory=list)  # independent variables

    def zone(self, value: float) -> str:
        lo_c, hi_c = self.control
        lo_a, hi_a = self.alarm
        lo_t, hi_t = self.trip
        if lo_c <= value <= hi_c:
            return "control"
        if lo_t <= value <= lo_a or hi_a <= value <= hi_t:
            return "trip"
        if lo_a <= value < lo_c or hi_c < value <= hi_a:
            return "alarm"
        return "trip"  # beyond trip band

    def status(self, value: float) -> str:
        lo_c, hi_c = self.control
        if value > hi_c:
            return "OVER"
        if value < lo_c:
            return "UNDER"
        return "OK"

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["control"] = list(self.control)
        d["alarm"] = list(self.alarm)
        d["trip"] = list(self.trip)
        return d


# ---- the five parameters (canonical) ----
PARAMETERS: Dict[str, Parameter] = {
    "temp": Parameter(
        id="temp", name="Reactor Temperature", short="Temperature", unit="°C",
        tag="TT-1202B", asset="BR-12",
        control=(36.5, 37.5), alarm=(36.0, 38.0), trip=(35.5, 38.5),
        model="Gradient Boosting (GBM)",
        drivers=["coolant_flow_rate", "jacket_temperature", "steam_valve_position",
                 "heat_exchanger_dp", "agitator_torque"],
    ),
    "ph": Parameter(
        id="ph", name="pH", short="pH", unit="pH",
        tag="AT-3401", asset="BR-12",
        control=(6.8, 7.2), alarm=(6.6, 7.4), trip=(6.4, 7.6),
        model="Random Forest",
        drivers=["acid_base_dose_rate", "co2_accumulation", "agitator_speed",
                 "probe_drift_mv", "dissolved_oxygen"],
    ),
    "press": Parameter(
        id="press", name="Differential Pressure", short="Pressure", unit="kPa",
        tag="PT-2201", asset="FIL-07",
        control=(100.0, 110.0), alarm=(96.0, 114.0), trip=(92.0, 118.0),
        model="Gradient Boosting (GBM)",
        drivers=["filter_dp", "gas_exhaust_flow", "pump_speed_hz",
                 "valve_position", "seal_integrity_index"],
    ),
    "cond": Parameter(
        id="cond", name="Conductivity (WFI)", short="Conductivity", unit="µS/cm",
        tag="CT-5501", asset="WFI-02",
        control=(700.0, 900.0), alarm=(640.0, 960.0), trip=(600.0, 1000.0),
        model="Support Vector Machine (RBF)",
        drivers=["water_flow_rate", "resin_bed_dp", "regeneration_cycle_count",
                 "toc_level", "feed_composition"],
    ),
    "hum": Parameter(
        id="hum", name="Humidity (Cleanroom)", short="Humidity", unit="%",
        tag="MT-6601", asset="CR-A1",
        control=(40.0, 55.0), alarm=(36.0, 59.0), trip=(32.0, 63.0),
        model="Neural Network (MLP)",
        drivers=["hvac_fan_speed", "cooling_coil_temp", "hepa_dp",
                 "outdoor_humidity", "door_open_count"],
    ),
}
PARAM_IDS = list(PARAMETERS.keys())
TAG_TO_PARAM = {p.tag: p.id for p in PARAMETERS.values()}
