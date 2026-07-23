#!/usr/bin/env python
"""Extraction of variables from a single ASTEC saving time step via pyodessa."""

import pyodessa as pyod  # noqa: E402
import numpy as np


class assas_odessa_step_extraction:
    """Extract variables from a single ASTEC saving time step."""

    def __init__(self) -> None:
        """Initialize the step extractor."""
        pass

    @staticmethod
    def extract_one_time_step(
        base: pyod.Base,
        fp_anonymization: dict,
        value_anonymization: dict
    ) -> dict:
        """Extract all variables from one saving time of a filtered astec database."""
        result = {}
        root = base._od_obj

        sequence = pyod.lib.odbase_get_odbase(root, "SEQUENCE", 1)

        # vessel_rupture_time
        if pyod.lib.odbase_card(sequence, "TRUP") > 0:
            value = pyod.lib.odbase_get_double(sequence, "TRUP", 1)
        else:
            value = np.nan
        result["vessel_rupture_time"] = value

        # vessel_release_time
        if pyod.lib.odbase_card(sequence, "TFP") > 0:
            value = pyod.lib.odbase_get_double(sequence, "TFP", 1)
        else:
            value = np.nan
        result["vessel_release_time"] = value

        # sensor_values
        nb_sensors = pyod.lib.odbase_card(root, "SENSOR")
        values = np.empty((nb_sensors), dtype=np.float64)
        for isens in range(pyod.lib.odbase_card(root, "SENSOR")):
            sensor = pyod.lib.odbase_get_odbase(root, "SENSOR", isens + 1)
            values[isens] = pyod.lib.odbase_get_double(sensor, "value", 1)
        result["sensor_values"] = values

        # operator actions
        names = [
            "freqsave",
            "opensrv",
            "p_u5",
            "t1_srv",
            "t2_srv",
            "t_fbseb",
            "t_u5",
            "tcss",
            "tendcalc",
            "tendssg2",
            "time_end",
            "tpesp",
            "tpessg",
            "tsg2tr",
        ]
        private = pyod.lib.odbase_get_odbase(root, "PRIVATE", 1)
        assaspar = pyod.lib.odbase_get_odbase(private, "ASSASpar", 1)
        for name in names:
            result[name] = pyod.lib.odbase_get_double(assaspar, name, 1)

        if pyod.lib.odbase_card(root, "CESAR_IO") > 0:
            cesar_io = pyod.lib.odbase_get_odbase(root, "CESAR_IO", 1)
            if pyod.lib.odbase_card(cesar_io, "MACROBEG") > 0:
                value = pyod.lib.odbase_get_double(cesar_io, "MACROBEG", 1)
                result["macro_ts_begin"] = value

                value = pyod.lib.odbase_get_double(cesar_io, "MACROEND", 1)
                result["macro_ts_end"] = value

                value = pyod.lib.odbase_get_double(cesar_io, "dtmacro", 1)
                result["macro_ts_duration"] = value

                value = pyod.lib.odbase_get_double(cesar_io, "STEPBEG", 1)
                result["micro_ts_begin"] = value

                value = pyod.lib.odbase_get_double(cesar_io, "STEPEND", 1)
                result["micro_ts_end"] = value

                value = pyod.lib.odbase_get_double(cesar_io, "dtfluid", 1)
                result["micro_ts_duration"] = value

                value = pyod.lib.odbase_get_int(cesar_io, "CONV", 1)
                result["conv_variable"] = value

                output = pyod.lib.odbase_get_odbase(cesar_io, "OUPUTS", 1)
                value = pyod.lib.odbase_get_int(output, "ITER", 1)
                result["iteration_info"] = value

                pyod.lib.odbase_get_odr1(output, "VARPRIM", 1)
                nb_varprim = 2233
                values = np.empty((nb_varprim), dtype=np.float64)
                for ivar in range(nb_varprim):
                    values[ivar] = pyod.lib.odr1_get(ivar + 1)
                result["variable_prime"] = values
            else:
                result["macro_ts_begin"] = np.array([np.nan])

                result["macro_ts_end"] = np.array([np.nan])

                result["macro_ts_duration"] = np.array([np.nan])

                result["micro_ts_begin"] = np.array([np.nan])

                result["micro_ts_end"] = np.array([np.nan])

                result["micro_ts_duration"] = np.array([np.nan])

                result["conv_variable"] = np.array([np.nan])

                result["iteration_info"] = np.array([np.nan])

                result["variable_prime"] = np.array([np.nan])

        else:
            result["macro_ts_begin"] = np.array([np.nan])

            result["macro_ts_end"] = np.array([np.nan])

            result["macro_ts_duration"] = np.array([np.nan])

            result["micro_ts_begin"] = np.array([np.nan])

            result["micro_ts_end"] = np.array([np.nan])

            result["micro_ts_duration"] = np.array([np.nan])

            result["conv_variable"] = np.array([np.nan])

            result["iteration_info"] = np.array([np.nan])
            result["variable_prime"] = np.array([np.nan])
        # PRIMARY

        if np.isnan(result["vessel_rupture_time"]):
            circuit = pyod.lib.odbase_get_odbase(root, "PRIMARY", 1)

        # WALL
        nb_walls = 92

        hdf5_names = [
            "phi_wtol_wall_primary",
            "phi_wtog_wall_primary",
            "phi_wtoi_wall_primary",
            "T_wall_primary",
            "Ts_wall_primary",
            "T_wall_primary_2",
            "Ts_wall_primary_2",
            "Fp_power_wall_primary",
        ]

        for vari in hdf5_names:
            result[vari] = np.full((nb_walls), dtype=np.float64, fill_value=np.nan)

        if np.isnan(result["vessel_rupture_time"]):
            for iwall in range(nb_walls):
                wall = pyod.lib.odbase_get_odbase(circuit, "WALL", iwall + 1)
                for ither in range(pyod.lib.odbase_card(wall, "THER")):
                    ther = pyod.lib.odbase_get_odbase(wall, "THER", ither + 1)
                    if ither == 0:
                        for k, v in {
                            "phi_wtol_wall_primary": "phi_wtol",
                            "phi_wtog_wall_primary": "phi_wtog",
                            "phi_wtoi_wall_primary": "phi_wtoi",
                            "T_wall_primary": "T_wall",
                            "Ts_wall_primary": "Ts_wall",
                        }.items():
                            vect_odr1 = pyod.lib.odbase_get_odr1(ther, v, 1)
                            result[k][iwall] = pyod.lib.odr1_get(vect_odr1, 0)
                    else:
                        for k, v in {
                            "T_wall_primary_2": "T_wall",
                            "Ts_wall_primary_2": "Ts_wall",
                        }.items():
                            vect_odr1 = pyod.lib.odbase_get_odr1(ther, v, 1)
                            result[k][iwall] = pyod.lib.odr1_get(vect_odr1, 0)

                # Power in WALLs
                value = pyod.lib.odbase_get_double(wall, "fp_power", 1)
                result["Fp_power_wall_primary"][iwall] = value

        # VOLUME
        nb_vol = 85

        hdf5_names = [
            "T_gas_primary_volume",
            "T_liq_primary_volume",
            "T_sat_primary_volume",
            "rho_liq_primary_volume",
            "m_steam_primary_volume",
            "m_liq_primary_volume",
            "P_primary_volume",
            "P_up_primary_volume",
            "P_steam_primary_volume",
            "P_H2_primary_volume",
            "P_saturation_primary_volume",
            "x_alfa_primary_volume",
            "x_steam_primary_volume",
        ]

        for vari in hdf5_names:
            result[vari] = np.full((nb_vol), dtype=np.float64, fill_value=np.nan)

        if np.isnan(result["vessel_rupture_time"]):
            for ivol in range(nb_vol):
                volume = pyod.lib.odbase_get_odbase(circuit, "VOLUME", ivol + 1)
                ther = pyod.lib.odbase_get_odbase(volume, "THER", 1)

                for count, v in enumerate(
                    [
                        "T_gas",
                        "T_liq",
                        "T_sat",
                        "rho_liq",
                        "m_steam",
                        "m_liq",
                        "P",
                        "P_UP",
                        "P_steam",
                        "P_h2",
                        "Psat",
                        "x_alfa",
                        "x_steam",
                    ]
                ):
                    vect_odr1 = pyod.lib.odbase_get_odr1(ther, v, 1)
                    if vect_odr1.value > 0:
                        result[hdf5_names[count]][ivol] = pyod.lib.odr1_get(
                            vect_odr1, 0
                        )
                    else:
                        result[hdf5_names[count]][ivol] = np.nan

        # JUNCTION
        nb_junc = 93

        hdf5_names = [
            "V_gas_junction_ther",
            "V_liq_junction_ther",
            "Q_m_liq_junction_ther",
        ]

        for vari in hdf5_names:
            result[vari] = np.full((nb_junc), dtype=np.float64, fill_value=np.nan)
        if np.isnan(result["vessel_rupture_time"]):
            for ijunc in range(nb_junc):
                junction = pyod.lib.odbase_get_odbase(circuit, "JUNCTION", ijunc + 1)
                ther = pyod.lib.odbase_get_odbase(junction, "THER", 1)

                for count, v in enumerate(["v_gas", "v_liq", "q_m_liq"]):
                    vect_odr1 = pyod.lib.odbase_get_odr1(ther, v, 1)
                    result[hdf5_names[count]][ijunc] = pyod.lib.odr1_get(vect_odr1, 0)

        # SECONDAR

        if np.isnan(result["vessel_rupture_time"]):
            circuit = pyod.lib.odbase_get_odbase(root, "SECONDAR", 1)

        # WALL
        nb_walls = 125

        hdf5_names = [
            "T_wall_secondar_wall",
            "Ts_wall_secondar_wall",
            "T_wall_secondar_wall_ther_2",
            "Ts_wall_secondar_wall_ther_2",
            "Fp_power_secondar_wall",
        ]

        for vari in hdf5_names:
            result[vari] = np.full((nb_walls), dtype=np.float64, fill_value=np.nan)

        if np.isnan(result["vessel_rupture_time"]):
            for iwall in range(nb_walls):
                wall = pyod.lib.odbase_get_odbase(circuit, "WALL", iwall + 1)
                for ither in range(pyod.lib.odbase_card(wall, "THER")):
                    ther = pyod.lib.odbase_get_odbase(wall, "THER", ither + 1)
                    if ither == 0:
                        for k, v in {
                            "T_wall_secondar_wall": "T_wall",
                            "Ts_wall_secondar_wall": "Ts_wall",
                        }.items():
                            vect_odr1 = pyod.lib.odbase_get_odr1(ther, v, 1)
                            result[k][iwall] = pyod.lib.odr1_get(vect_odr1, 0)
                    else:
                        for k, v in {
                            "T_wall_secondar_wall_ther_2": "T_wall",
                            "Ts_wall_secondar_wall_ther_2": "Ts_wall",
                        }.items():
                            vect_odr1 = pyod.lib.odbase_get_odr1(ther, v, 1)
                            result[k][iwall] = pyod.lib.odr1_get(vect_odr1, 0)

                # Power in WALLs
                value = pyod.lib.odbase_get_double(wall, "fp_power", 1)
                result["Fp_power_secondar_wall"][iwall] = value

        # VOLUME
        nb_vol = 73

        hdf5_names = [
            "T_gas_secondar_volume",
            "T_liq_secondar_volume",
            "T_sat_secondar_volume",
            "rho_gas_secondar_volume",
            "rho_liq_secondar_volume",
            "m_steam_secondar_volume",
            "m_liq_secondar_volume",
            "P_secondar_volume",
            "P_steam_secondar_volume",
            "P_H2_secondar_volume",
            "x_alfa_secondar_volume",
            "x_alfa1_secondar_volume",
        ]

        for vari in hdf5_names:
            result[vari] = np.full((nb_vol), dtype=np.float64, fill_value=np.nan)

        if np.isnan(result["vessel_rupture_time"]):
            for ivol in range(nb_vol):
                volume = pyod.lib.odbase_get_odbase(circuit, "VOLUME", ivol + 1)
                ther = pyod.lib.odbase_get_odbase(volume, "THER", 1)

                for count, v in enumerate(
                    [
                        "T_gas",
                        "T_liq",
                        "T_sat",
                        "rho_gas",
                        "rho_liq",
                        "m_steam",
                        "m_liq",
                        "P",
                        "P_steam",
                        "P_h2",
                        "x_alfa",
                        "x_alfa1",
                    ]
                ):
                    vect_odr1 = pyod.lib.odbase_get_odr1(ther, v, 1)
                    if vect_odr1.value > 0:
                        result[hdf5_names[count]][ivol] = pyod.lib.odr1_get(
                            vect_odr1, 0
                        )
                    else:
                        result[hdf5_names[count]][ivol] = np.nan

        # JUNCTION
        nb_junc = 76

        hdf5_names = [
            "V_gas_secondar_junction_ther",
            "V_liq_secondar_junction_ther",
            "Q_m_stea_secondar_junction_ther",
            "Q_m_liq_secondar_junction_ther",
        ]

        for vari in hdf5_names:
            result[vari] = np.full((nb_junc), dtype=np.float64, fill_value=np.nan)
        if np.isnan(result["vessel_rupture_time"]):
            for ijunc in range(nb_junc):
                junction = pyod.lib.odbase_get_odbase(circuit, "JUNCTION", ijunc + 1)
                ther = pyod.lib.odbase_get_odbase(junction, "THER", 1)

                for count, v in enumerate(["v_gas", "v_liq", "q_m_stea", "q_m_liq"]):
                    vect_odr1 = pyod.lib.odbase_get_odr1(ther, v, 1)
                    result[hdf5_names[count]][ijunc] = pyod.lib.odr1_get(vect_odr1, 0)

        # VESSEL

        # MESH related data
        hdf5_names = [
            "P_vessel",
            "P_H2_vessel",
            "P_steam_vessel",
            "T_gas_vessel",
            "T_liq_vessel",
            "T_sat_vessel",
            "m_gas_vessel",
            "m_liq_vessel_mesh",
            "Q_liq_vap_vessel",
            "rho_gas_vessel",
            "rho_liq_vessel",
            "x_alfa_vessel",
            "porosity_vessel",
            "V_deb_vessel",
            "V_mag_vessel",
            "m_magma_vessel",
            "m_debris_0_vessel",
            "m_debris_1_vessel",
        ]
        nb_mesh = 76
        for vari in hdf5_names:
            result[vari] = np.full((nb_mesh), dtype=np.float64, fill_value=np.nan)

        if np.isnan(result["vessel_rupture_time"]):
            magma_comp = [
                None,
                376,
                381,
                386,
                390,
                399,
                405,
                411,
                418,
                423,
                432,
                442,
                458,
                475,
                478,
                487,
                497,
                513,
                530,
                533,
                542,
                552,
                568,
                585,
                588,
                597,
                606,
                621,
                637,
                640,
                649,
                659,
                675,
                692,
                695,
                704,
                714,
                730,
                747,
                750,
                759,
                769,
                785,
                802,
                805,
                814,
                823,
                838,
                854,
                857,
                866,
                876,
                892,
                909,
                912,
                921,
                931,
                947,
                964,
                967,
                976,
                986,
                1002,
                1019,
                1022,
                1031,
                1041,
                1057,
                1074,
                1077,
                1086,
                1091,
                1096,
                1102,
                1107,
                1116,
            ]
            debris1_comp = [
                None,
                374,
                379,
                384,
                None,
                None,
                403,
                409,
                416,
                None,
                None,
                440,
                456,
                473,
                None,
                None,
                495,
                511,
                528,
                None,
                None,
                550,
                566,
                583,
                None,
                None,
                604,
                619,
                635,
                None,
                None,
                657,
                673,
                690,
                None,
                None,
                712,
                728,
                745,
                None,
                None,
                767,
                783,
                800,
                None,
                None,
                821,
                836,
                852,
                None,
                None,
                874,
                890,
                907,
                None,
                None,
                929,
                945,
                962,
                None,
                None,
                984,
                1000,
                1017,
                None,
                None,
                1039,
                1055,
                1072,
                None,
                None,
                1089,
                1094,
                1100,
                None,
                None,
            ]
            debris2_comp = [
                None,
                375,
                380,
                385,
                None,
                None,
                404,
                410,
                417,
                None,
                None,
                441,
                457,
                474,
                None,
                None,
                496,
                512,
                529,
                None,
                None,
                551,
                567,
                584,
                None,
                None,
                605,
                620,
                636,
                None,
                None,
                658,
                674,
                691,
                None,
                None,
                713,
                729,
                746,
                None,
                None,
                768,
                784,
                801,
                None,
                None,
                822,
                837,
                853,
                None,
                None,
                875,
                891,
                908,
                None,
                None,
                930,
                946,
                963,
                None,
                None,
                985,
                1001,
                1018,
                None,
                None,
                1040,
                1056,
                1073,
                None,
                None,
                1090,
                1095,
                1101,
                None,
                None,
            ]
            vessel = pyod.lib.odbase_get_odbase(root, "VESSEL", 1)

            for imesh in range(nb_mesh):
                mesh = pyod.lib.odbase_get_odbase(vessel, "MESH", imesh + 1)
                ther = pyod.lib.odbase_get_odbase(mesh, "THER", 1)
                list_names = dict(
                    P="P_vessel",
                    P_h2="P_H2_vessel",
                    P_steam="P_steam_vessel",
                    T_gas="T_gas_vessel",
                    T_liq="T_liq_vessel",
                    T_sat="T_sat_vessel",
                    m_gas="m_gas_vessel",
                    m_liq="m_liq_vessel_mesh",
                    q_m_list="Q_liq_vap_vessel",
                    rho_gas="rho_gas_vessel",
                    rho_liq="rho_liq_vessel",
                    x_alfa="x_alfa_vessel",
                )
                for k, v in list_names.items():
                    vect_odr1 = pyod.lib.odbase_get_odr1(ther, k, 1)
                    result[v][imesh] = pyod.lib.odr1_get(vect_odr1, 0)

                for k, v in dict(
                    POROSITY="porosity_vessel",
                    VOLFDEB="V_deb_vessel",
                    VOLFMAG="V_mag_vessel",
                ).items():
                    value = pyod.lib.odbase_get_double(mesh, k, 1)
                    result[v][imesh] = value

                for k, v in dict(
                    m_magma_vessel=magma_comp[imesh],
                    m_debris_0_vessel=debris1_comp[imesh],
                    m_debris_1_vessel=debris2_comp[imesh],
                ).items():
                    if not v:
                        continue
                    comp = pyod.lib.odbase_get_odbase(vessel, "COMP", v)
                    value = pyod.lib.odbase_get_double(comp, "M", 1)
                    result[k][imesh] = value

        # COMP information
        clad_comp = [
            434,
            444,
            461,
            489,
            499,
            516,
            544,
            554,
            571,
            599,
            608,
            624,
            651,
            661,
            678,
            706,
            716,
            733,
            761,
            771,
            788,
            816,
            825,
            841,
            868,
            878,
            895,
            923,
            933,
            950,
            978,
            988,
            1005,
            1033,
            1043,
            1060,
        ]
        fuel_comp = [
            433,
            443,
            460,
            488,
            498,
            515,
            543,
            553,
            570,
            598,
            607,
            623,
            650,
            660,
            677,
            705,
            715,
            732,
            760,
            770,
            787,
            815,
            824,
            840,
            867,
            877,
            894,
            922,
            932,
            949,
            977,
            987,
            1004,
            1032,
            1042,
            1059,
        ]
        bono_comp = [400, 406, 413]
        crod_comp = [
            435,
            445,
            448,
            451,
            462,
            465,
            468,
            490,
            500,
            503,
            506,
            517,
            520,
            523,
            545,
            555,
            558,
            561,
            572,
            575,
            578,
            600,
            609,
            612,
            615,
            625,
            628,
            631,
            652,
            662,
            665,
            668,
            679,
            682,
            685,
            707,
            717,
            720,
            723,
            734,
            737,
            740,
            762,
            772,
            775,
            778,
            789,
            792,
            795,
            817,
            826,
            829,
            832,
            842,
            845,
            848,
            869,
            879,
            882,
            885,
            896,
            899,
            902,
            924,
            934,
            937,
            940,
            951,
            954,
            957,
            979,
            989,
            992,
            995,
            1006,
            1009,
            1012,
            1034,
            1044,
            1047,
            1050,
            1061,
            1064,
            1067,
        ]
        clad_crod_comp = [
            436,
            446,
            449,
            452,
            463,
            466,
            469,
            491,
            501,
            504,
            507,
            518,
            521,
            524,
            546,
            556,
            559,
            562,
            573,
            576,
            579,
            601,
            610,
            613,
            616,
            626,
            629,
            632,
            653,
            663,
            666,
            669,
            680,
            683,
            686,
            708,
            718,
            721,
            724,
            735,
            738,
            741,
            763,
            773,
            776,
            779,
            790,
            793,
            796,
            818,
            827,
            830,
            833,
            843,
            846,
            849,
            870,
            880,
            883,
            886,
            897,
            900,
            903,
            925,
            935,
            938,
            941,
            952,
            955,
            958,
            980,
            990,
            993,
            996,
            1007,
            1010,
            1013,
            1035,
            1045,
            1048,
            1051,
            1062,
            1065,
            1068,
        ]
        tguide_comp = [
            437,
            447,
            450,
            453,
            464,
            467,
            470,
            492,
            502,
            505,
            508,
            519,
            522,
            525,
            547,
            557,
            560,
            563,
            574,
            577,
            580,
            602,
            611,
            614,
            617,
            627,
            630,
            633,
            654,
            664,
            667,
            670,
            681,
            684,
            687,
            709,
            719,
            722,
            725,
            736,
            739,
            742,
            764,
            774,
            777,
            780,
            791,
            794,
            797,
            819,
            828,
            831,
            834,
            844,
            847,
            850,
            871,
            881,
            884,
            887,
            898,
            901,
            904,
            926,
            936,
            939,
            942,
            953,
            956,
            959,
            981,
            991,
            994,
            997,
            1008,
            1011,
            1014,
            1036,
            1046,
            1049,
            1052,
            1063,
            1066,
            1069,
        ]
        tinst_comp = [
            438,
            454,
            471,
            493,
            509,
            526,
            548,
            564,
            581,
            603,
            618,
            634,
            655,
            671,
            688,
            710,
            726,
            743,
            765,
            781,
            798,
            820,
            835,
            851,
            872,
            888,
            905,
            927,
            943,
            960,
            982,
            998,
            1015,
            1037,
            1053,
            1070,
        ]
        grid_comp = [
            439,
            455,
            472,
            494,
            510,
            527,
            549,
            565,
            582,
            656,
            672,
            689,
            711,
            727,
            744,
            766,
            782,
            799,
            873,
            889,
            906,
            928,
            944,
            961,
            983,
            999,
            1016,
            1038,
            1054,
            1071,
        ]
        LP_wall_comp = [
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            26,
            27,
            28,
            29,
            30,
            31,
            32,
            33,
            34,
            35,
            36,
        ]
        LP_corium_comp = [361, 362, 363, 364, 365, 370, 371]

        fuel_dict = {"clad": clad_comp, "fuel": fuel_comp}
        component_dict = {
            "bono": bono_comp,
            "crod": crod_comp,
            "clad_crod": clad_crod_comp,
            "tguide": tguide_comp,
            "tinst": tinst_comp,
            "grid": grid_comp,
            "LP_wall": LP_wall_comp,
            "LP_corium": LP_corium_comp,
        }
        vari_names = ["M", "T", "H"]
        state = {
            "COMPACT": 0.0,
            "PERFORAT": 1.0,
            "CRACKED": 2.0,
            "DISLOCAT": 3.0,
            "ABSENT": 4.0,
        }

        for k, v in fuel_dict.items():
            result["T_comp_" + k] = np.full(len(v), dtype=np.float64, fill_value=np.nan)
            result["m_comp_" + k] = np.full(len(v), dtype=np.float64, fill_value=np.nan)
            result["H_comp_" + k] = np.full(len(v), dtype=np.float64, fill_value=np.nan)
            result["state_" + k] = np.full(len(v), dtype=np.float64, fill_value=np.nan)

        for k, v in component_dict.items():
            result["T_comp_" + k] = np.full(len(v), dtype=np.float64, fill_value=np.nan)
            result["m_comp_" + k] = np.full(len(v), dtype=np.float64, fill_value=np.nan)
            result["H_comp_" + k] = np.full(len(v), dtype=np.float64, fill_value=np.nan)

        if np.isnan(result["vessel_rupture_time"]):
            for k, v in fuel_dict.items():
                for count, icomp in enumerate(v):
                    comp = pyod.lib.odbase_get_odbase(vessel, "COMP", icomp)
                    value = pyod.lib.odbase_get_double(comp, "T", 1)
                    result["T_comp_" + k][count] = value
                    value = pyod.lib.odbase_get_double(comp, "M", 1)
                    result["m_comp_" + k][count] = value
                    odrg_vect = pyod.lib.odbase_get_odrg(comp, "PROP", 1)
                    value = pyod.lib.odrg_get(odrg_vect, "H")
                    result["H_comp_" + k][count] = value
                    val_stat = pyod.lib.odbase_get_string(comp, "STAT", 1)
                    result["state_" + k][count] = state[val_stat]
            for k, v in component_dict.items():
                for count, icomp in enumerate(v):
                    comp = pyod.lib.odbase_get_odbase(vessel, "COMP", icomp)
                    value = pyod.lib.odbase_get_double(comp, "T", 1)
                    result["T_comp_" + k][count] = value
                    value = pyod.lib.odbase_get_double(comp, "M", 1)
                    result["m_comp_" + k][count] = value
                    odrg_vect = pyod.lib.odbase_get_odrg(comp, "PROP", 1)
                    value = pyod.lib.odrg_get(odrg_vect, "H")
                    result["H_comp_" + k][count] = value

        # ACTISUM
        if np.isnan(result["vessel_rupture_time"]):
            fp_heat = pyod.lib.odbase_get_odbase(root, "FP_HEAT", 1)
            vess = pyod.lib.odbase_get_odbase(fp_heat, "VESSEL", 1)
            vect_odr1 = pyod.lib.odbase_get_odr1(vess, "ACTISUM", 1)
            value = pyod.lib.odr1_get(vect_odr1, 0)
        else:
            value = np.nan
        result["FP_A_heat"] = value

        # VESSEL:GENERAL
        names = {
            "PRODH2": "m_cum_H2",
            "SATUMX": "sat_core_mesh",
            "TOTMAMAG": "m_tot_cor",
            "TOTMADEB": "m_tot_deb",
        }
        for value in names.values():
            result[value] = np.nan
        if np.isnan(result["vessel_rupture_time"]):
            general = pyod.lib.odbase_get_odbase(vessel, "GENERAL", 1)
            for k, v in names.items():
                value = pyod.lib.odbase_get_double(general, k, 1)
                result[v] = value

        # FACE
        nb_face = 140
        vari_names = {
            "Q_m_liq_face": "q_m_liq",
            "V_gas_face": "v_gas",
            "V_liq_face": "v_liq",
        }

        for vari in vari_names.keys():
            result[vari] = np.full((nb_face), dtype=np.float64, fill_value=np.nan)

        if np.isnan(result["vessel_rupture_time"]):
            for iface in range(nb_face):
                face = pyod.lib.odbase_get_odbase(vessel, "FACE", iface + 1)
                ther = pyod.lib.odbase_get_odbase(face, "THER", 1)
                for k, v in vari_names.items():
                    vect_odr1 = pyod.lib.odbase_get_odr1(ther, v, 1)
                    value = pyod.lib.odr1_get(vect_odr1, 0)
                    result[k][iface] = value

        flow_idx = [1, 2]
        feedwate_idx = [
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            24,
            25,
            26,
            27,
            28,
            29,
            30,
            31,
            60,
            61,
            62,
            63,
            64,
        ]
        break_idx = [
            19,
            20,
            21,
            32,
            33,
            34,
            35,
            36,
            37,
            38,
            39,
            40,
            41,
            42,
            43,
            44,
            45,
            46,
            47,
            48,
            49,
            50,
            51,
            52,
            53,
            54,
            55,
            56,
            57,
            58,
            68,
            69,
            75,
            76,
            77,
            78,
            79,
            80,
        ]
        heat_idx = [22, 23, 66, 67]
        source_idx = [59, 70, 71, 72, 73]
        mcci_idx = [65]

        flow_fp_list = [
            "Ac",
            "Ag",
            "Am",
            "As",
            "Ba",
            "Br",
            "Cd",
            "Ce",
            "Cm",
            "Cs",
            "Cu",
            "Dy",
            "Er",
            "Eu",
            "Ga",
            "Gd",
            "Ge",
            "Ho",
            "I",
            "In",
            "Kr",
            "La",
            "Mo",
            "Nb",
            "Nd",
            "Np",
            "Pa",
            "Pd",
            "Pm",
            "Pr",
            "Pu",
            "Ra",
            "Rb",
            "Re",
            "Rh",
            "Ru",
            "Sb",
            "Se",
            "Sm",
            "Sn",
            "Sr",
            "Tb",
            "Tc",
            "Te",
            "Th",
            "Tl",
            "Tm",
            "U",
            "Xe",
            "Y",
            "Yb",
            "Zn",
            "Zr",
            "SMAG",
            "SMB",
            "SMC",
            "SMCD",
            "SMCL",
            "SMCR",
            "SMFE",
            "SMIN",
            "SMMN",
            "SMNI",
            "SMSI",
            "SMSN",
            "SMZR",
        ]
        flow_fp_idx = [
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            26,
            27,
            28,
            29,
            30,
            31,
            32,
            33,
            34,
            35,
            36,
            37,
            38,
            39,
            40,
            41,
            42,
            43,
            44,
            45,
            46,
            47,
            48,
            49,
            50,
            51,
            52,
            53,
            54,
            55,
            56,
            57,
            58,
            59,
            60,
            61,
            62,
            63,
            64,
            65,
            66,
            67,
            68,
            69,
        ]
        break_fp_list = [
            "Ag",
            "Am",
            "As",
            "B",
            "Ba",
            "Br",
            "C",
            "Cd",
            "Ce",
            "Cl",
            "Cm",
            "Cr",
            "Cs",
            "Cu",
            "Eu",
            "Fe",
            "Ga",
            "Gd",
            "Ge",
            "I",
            "In",
            "Kr",
            "La",
            "Mn",
            "Mo",
            "Nb",
            "Nd",
            "Ni",
            "Np",
            "Pd",
            "Pm",
            "Pr",
            "Pu",
            "Rb",
            "Re",
            "Rh",
            "Ru",
            "Sb",
            "Se",
            "Si",
            "Sm",
            "Sn",
            "Sr",
            "Tc",
            "Te",
            "Th",
            "U",
            "Xe",
            "Y",
            "Zn",
            "Zr",
            "Ac",
            "Dy",
            "Er",
            "Ho",
            "Pa",
            "Ra",
            "Tb",
            "Tl",
            "Tm",
            "Yb",
            "I_NG",
            "I2",
            "SMAG",
            "SMB",
            "SMC",
            "SMCD",
            "SMCL",
            "SMCR",
            "SMFE",
            "SMIN",
            "SMMN",
            "SMNI",
            "SMSI",
            "SMSN",
            "SMZR",
        ]
        break_fp_idx = [
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            26,
            27,
            28,
            29,
            30,
            31,
            32,
            33,
            34,
            35,
            36,
            37,
            38,
            39,
            40,
            41,
            42,
            43,
            44,
            45,
            46,
            47,
            48,
            49,
            50,
            51,
            52,
            53,
            54,
            55,
            56,
            57,
            58,
            59,
            60,
            61,
            62,
            63,
            64,
            65,
            66,
            67,
            68,
            69,
            70,
            71,
            72,
            73,
            74,
            75,
            76,
            77,
            78,
            79,
        ]

        if not fp_anonymization:
            fp_anonymization ={name : name for name in ['Ac', 'Ag', 'Am', 'As', 'B', 'Ba', 'Br', 'C', 'Cd', 'Ce', 'Cl', 'Cm', 'Cr', 'Cs', 'Cu', 'Dy', 'Er', 'Eu', 'Fe', 'Ga', 'Gd', 'Ge', 'Ho', 'I', 'I2', 'I_NG', 'In', 'Kr', 'La', 'Mn', 'Mo', 'Nb', 'Nd', 'Ni', 'Np', 'Pa', 'Pd', 'Pm', 'Pr', 'Pu', 'Ra', 'Rb', 'Re', 'Rh', 'Ru', 'SMAG', 'SMB', 'SMC', 'SMCD', 'SMCL', 'SMCR', 'SMFE', 'SMIN', 'SMMN', 'SMNI', 'SMSI', 'SMSN', 'SMZR', 'Sb', 'Se', 'Si', 'Sm', 'Sn', 'Sr', 'Tb', 'Tc', 'Te', 'Th', 'Tl', 'Tm', 'U', 'Xe', 'Y', 'Yb', 'Zn', 'Zr']}


        var_m = [
            "Mwater_cum_connecti_",
            "Msteam_cum_connecti_",
            "Mh2_cum_connecti_",
        ]  # FLOW : -1
        var_q = ["Qwater_connecti_", "Qsteam_connecti_", "Qh2_connecti_"]  # QMAV
        var_h = [
            "Hwater_cum_connecti_",
            "Hsteam_cum_connecti_",
            "Hh2_cum_connecti_",
        ]  # FLOW : 0
        var_PT = {"T": "T_connecti_", "P": "P_connecti_"}

        # FLOW CONNECTI
        for varis in [var_m, var_q, var_h]:
            for vari in varis:
                result[vari + "flow"] = np.full(
                    (len(flow_idx)), dtype=np.float64, fill_value=np.nan
                )
        for fp in flow_fp_list:
            result["Q_fp_" + fp_anonymization[fp] + "_connecti_flow"] = np.full(
                (1), dtype=np.float64, fill_value=np.nan
            )
            result["m_cum_fp_" + fp_anonymization[fp] + "_connecti_flow"] = np.full(
                (1), dtype=np.float64, fill_value=np.nan
            )

        for count, iconn in enumerate(flow_idx):
            connecti = pyod.lib.odbase_get_odbase(root, "CONNECTI", iconn)

            for isource in range(3):
                source = pyod.lib.odbase_get_odbase(connecti, "SOURCE", isource + 1)
                # verif que la SOURCE est de TYPE FLUID ?
                source_type = pyod.lib.odbase_get_string(source, "TYPE", 1)
                if source_type != "FLUID":
                    continue
                odr1_vect = pyod.lib.odbase_get_odr1(source, "FLOW", 1)
                value = pyod.lib.odr1_get(odr1_vect, -1)
                result[var_m[isource] + "flow"][count] = value

                value = pyod.lib.odbase_get_double(source, "QMAV", 1)
                result[var_q[isource] + "flow"][count] = value

                value = pyod.lib.odr1_get(odr1_vect, 0)
                result[var_h[isource] + "flow"][count] = value
            nb_sources = pyod.lib.odcard(connecti, "SOURCE")
            if nb_sources > 3:
                assert nb_sources == 69
                for idx, fp in enumerate(flow_fp_list):
                    source = pyod.lib.odbase_get_odbase(
                        connecti, "SOURCE", flow_fp_idx[idx]
                    )
                    value = pyod.lib.odbase_get_double(source, "QMAV", 1)
                    result["Q_fp_" + fp_anonymization[fp] + "_connecti_flow"][count] = value
                    odr1_vect = pyod.lib.odbase_get_odr1(
                        source, "FLOW", 1
                    )  # WARNING: FLOW differs from MTOT...
                    value = pyod.lib.odr1_get(odr1_vect, 0)
                    result["m_cum_fp_" + fp_anonymization[fp] + "_connecti_flow"][count] = value

        # BREAK CONNECTI
        nb_sources = 80
        for varis in [var_m, var_q, var_h, ["T_connecti_", "P_connecti_"]]:
            for vari in varis:
                result[vari + "break"] = np.full(
                    (len(break_idx)), dtype=np.float64, fill_value=np.nan
                )
        for fp in break_fp_list:
<<<<<<< HEAD
            result["Q_fp_" + fp_anonymization[fp] + "_connecti_break"] = np.full(
                (len(break_idx)), dtype=np.float64, fill_value=np.nan
            )

        for count, iconn in enumerate(break_idx):
            connecti = pyod.lib.odbase_get_odbase(root, "CONNECTI", iconn)
            for k, v in var_PT.items():
                value = pyod.lib.odbase_get_double(connecti, k, 1)
                if value > 0.0:
                    result[v + "break"][count] = value
            nb_sources_conn = pyod.lib.odbase_card(connecti, "SOURCE")
            for isource in range(nb_sources_conn):
                source = pyod.lib.odbase_get_odbase(connecti, "SOURCE", isource + 1)
                odr1_vect = pyod.lib.odbase_get_odr1(source, "FLOW", 1)
                if isource < 3:
                    value = pyod.lib.odr1_get(odr1_vect, -1)
                    result[var_m[isource] + "break"][count] = value

                    value = pyod.lib.odbase_get_double(source, "QMAV", 1)
                    result[var_q[isource] + "break"][count] = value

                    value = pyod.lib.odr1_get(odr1_vect, 0)
                    result[var_h[isource] + "break"][count] = value
            nb_sources = pyod.lib.odcard(connecti, "SOURCE")
            if nb_sources > 3:
                assert nb_sources == 80
                for idx, fp in enumerate(break_fp_list):
                    source = pyod.lib.odbase_get_odbase(
                        connecti, "SOURCE", break_fp_idx[idx]
                    )
                    value = pyod.lib.odbase_get_double(source, "QMAV", 1)
                    result["Q_fp_" + fp_anonymization[fp] + "_connecti_break"][count] = value

        # FEEDWATE CONNECTI
        for vari in [
            "Mwater_cum_connecti_feedwater",
            "Qwater_connecti_feedwater",
            "Power_connecti_feedwater",
        ]:
            result[vari] = np.empty((len(feedwate_idx)), dtype=np.float64)

        for count, iconn in enumerate(feedwate_idx):
            connecti = pyod.lib.odbase_get_odbase(root, "CONNECTI", iconn)
            value = pyod.lib.odbase_get_double(connecti, "Mwater", 1)
            result["Mwater_cum_connecti_feedwater"][count] = value

            source = pyod.lib.odbase_get_odbase(connecti, "SOURCE", 1)
            odr1_vect = pyod.lib.odbase_get_odr1(source, "FLOW", 1)
            # Version Raph : value = pyod.lib.odbase_get_double(connecti,'Qwater',1)
            value = pyod.lib.odr1_get(odr1_vect, -1)
            result["Qwater_connecti_feedwater"][count] = value

            # Power
            value = pyod.lib.odr1_get(odr1_vect, 0)
            result["Power_connecti_feedwater"][count] = value

        # HEAT CONNECTI
        for vari in ["T_connecti_heat", "P_connecti_heat", "Power_connecti_heat"]:
            result[vari] = np.empty((len(heat_idx)), dtype=np.float64)

        for count, iconn in enumerate(heat_idx):
            connecti = pyod.lib.odbase_get_odbase(root, "CONNECTI", iconn)
            for k, v in var_PT.items():
                value = pyod.lib.odbase_get_double(connecti, k, 1)
                result[v + "heat"][count] = value

            heat = pyod.lib.odbase_get_odbase(connecti, "HEAT", 1)
            odr1_vect = pyod.lib.odbase_get_odr1(heat, "FLUX", 1)
            value = pyod.lib.odr1_get(odr1_vect, 0)
            result["Power_connecti_heat"][count] = value

        # SOURCE CONNECTI

        result["Qsteam_connecti_source"] = np.empty(len(source_idx), dtype=np.float64)

        for count, iconn in enumerate(source_idx):
            connecti = pyod.lib.odbase_get_odbase(root, "CONNECTI", iconn)
            # Qsteam
            value = pyod.lib.odbase_get_double(connecti, "Qsteam", 1)
            result["Qsteam_connecti_source"][count] = value

        # MCCI CONNECTI
        source_names = ["co", "co2", "h2", "steam", "o2"]
        for vari in source_names:
            result["Q" + vari + "_connecti_mcci"] = np.full(
                (len(mcci_idx)), dtype=np.float64, fill_value=np.nan
            )
            result["T" + vari + "_connecti_mcci"] = np.full(
                (len(mcci_idx)), dtype=np.float64, fill_value=np.nan
            )
            result["P" + vari + "_connecti_mcci"] = np.full(
                (len(mcci_idx)), dtype=np.float64, fill_value=np.nan
            )
        for vari in [
            "P_connecti_mcci",
            "T_connecti_mcci",
            "H0_connecti_mcci",
            "H1_connecti_mcci",
        ]:
            result[vari] = np.full((len(mcci_idx)), dtype=np.float64, fill_value=np.nan)

        connecti = pyod.lib.odbase_get_odbase(root, "CONNECTI", mcci_idx[0])
        for k, v in var_PT.items():
            value = pyod.lib.odbase_get_double(connecti, k, 1)
            result[v + "mcci"][0] = value

        nb_sources_conn = pyod.lib.odbase_card(connecti, "SOURCE")
        for isource in range(nb_sources_conn):
            source = pyod.lib.odbase_get_odbase(connecti, "SOURCE", isource + 1)
            odr1_vect = pyod.lib.odbase_get_odr1(source, "FLOW", 1)
            # Mass flow rate
            value = pyod.lib.odr1_get(odr1_vect, -2)
            result["Q" + source_names[isource] + "_connecti_mcci"][0] = value
            # Temperature
            value = pyod.lib.odr1_get(odr1_vect, -1)
            result["T" + source_names[isource] + "_connecti_mcci"][0] = value
            # Pressure
            value = pyod.lib.odr1_get(odr1_vect, 0)
            result["P" + source_names[isource] + "_connecti_mcci"][0] = value
        # Enthalpy
        nb_heat_conn = pyod.lib.odbase_card(connecti, "HEAT")
        for iheat in range(nb_heat_conn):
            heat = pyod.lib.odbase_get_odbase(connecti, "HEAT", iheat + 1)
            odr1_vect = pyod.lib.odbase_get_odr1(heat, "FLUX", 1)
            value = pyod.lib.odr1_get(odr1_vect, 0)
            result["H" + str(iheat) + "_connecti_mcci"][0] = value

        # containment_liq_vel_conn and containment_gas_vel_conn
        loadtime = pyod.lib.odbase_get_double(root, "LOADTIME", 1)
        if loadtime == 0.0:
            liq_values = np.full(68, dtype=np.float64, fill_value=np.nan)
            gas_values = np.full(68, dtype=np.float64, fill_value=np.nan)
        else:
            containm = pyod.lib.odbase_get_odbase(root, "CONTAINM", 1)
            nb_conn = pyod.lib.odbase_card(containm, "CONN")
            liq_values = np.full(nb_conn, dtype=np.float64, fill_value=np.nan)
            gas_values = np.full(nb_conn, dtype=np.float64, fill_value=np.nan)
            for iconn in range(pyod.lib.odbase_card(containm, "CONN")):
                conn = pyod.lib.odbase_get_odbase(containm, "CONN", iconn + 1)
                vf = pyod.lib.odbase_get_odr1(conn, "VF", 1)
                liq_values[iconn] = pyod.lib.odr1_get(vf, 0)
                vg = pyod.lib.odbase_get_odr1(conn, "VG", 1)
                gas_values[iconn] = pyod.lib.odr1_get(vg, 0)
        result["containment_liq_vel_conn"] = liq_values
        result["containment_gas_vel_conn"] = gas_values

        # containment_wall_temp
        containm = pyod.lib.odbase_get_odbase(root, "CONTAINM", 1)
        nb_wall = pyod.lib.odbase_card(containm, "WALL")
        wall_size = 20
        loadtime = pyod.lib.odbase_get_double(root, "LOADTIME", 1)
        if loadtime == 0.0:
            value = np.full((nb_wall, wall_size), dtype=np.float64, fill_value=np.nan)
        else:
            value = np.empty((nb_wall, wall_size), dtype=np.float64)
            for iwall in range(pyod.lib.odbase_card(containm, "WALL")):
                wall = pyod.lib.odbase_get_odbase(containm, "WALL", iwall + 1)
                if pyod.lib.odbase_card(wall, "SLAB") > 0:
                    slab = pyod.lib.odbase_get_odbase(wall, "SLAB", 1)
                    htemp = pyod.lib.odbase_get_odr1(slab, "HTEM", 1)
                    nb_val = pyod.lib.odr1_card(htemp)
                    assert nb_val == wall_size + 1  # first is time
                    for ival in range(2, nb_val + 1):
                        value[iwall, ival - 2] = pyod.lib.odr1_get(htemp, ival)
        result["containment_wall_temp"] = value

        containm = pyod.lib.odbase_get_odbase(root, "CONTAINM", 1)
        nb_zones = pyod.lib.odbase_card(containm, "ZONE")
        PRES = np.full(nb_zones, dtype=np.float64, fill_value=np.nan)
        TFLU = np.full(nb_zones, dtype=np.float64, fill_value=np.nan)
        WLEV = np.full(nb_zones, dtype=np.float64, fill_value=np.nan)
        TLIQ = np.full(nb_zones, dtype=np.float64, fill_value=np.nan)
        XCO2 = np.full(nb_zones, dtype=np.float64, fill_value=np.nan)
        XCO = np.full(nb_zones, dtype=np.float64, fill_value=np.nan)
        XH2 = np.full(nb_zones, dtype=np.float64, fill_value=np.nan)
        XH2O = np.full(nb_zones, dtype=np.float64, fill_value=np.nan)
        XN2 = np.full(nb_zones, dtype=np.float64, fill_value=np.nan)
        XO2 = np.full(nb_zones, dtype=np.float64, fill_value=np.nan)

        for izone in range(pyod.lib.odbase_card(containm, "ZONE")):
            zone = pyod.lib.odbase_get_odbase(containm, "ZONE", izone + 1)
            if pyod.lib.odbase_card(zone, "THER") > 0:
                ther = pyod.lib.odbase_get_odbase(zone, "THER", 1)

                # average_total_pressure
                if pyod.lib.odbase_card(ther, "PRES") > 0:
                    r1 = pyod.lib.odbase_get_odr1(ther, "PRES", 1)
                    PRES[izone] = pyod.lib.odr1_get(r1, 0)

                # average_fluid_temperature
                if pyod.lib.odbase_card(ther, "TFLU") > 0:
                    r1 = pyod.lib.odbase_get_odr1(ther, "TFLU", 1)
                    TFLU[izone] = pyod.lib.odr1_get(r1, 0)

                # water_level
                if pyod.lib.odbase_card(ther, "WLEV") > 0:
                    r1 = pyod.lib.odbase_get_odr1(ther, "WLEV", 1)
                    WLEV[izone] = pyod.lib.odr1_get(r1, 0)

                # average_liquid_temperature
                if pyod.lib.odbase_card(ther, "TLIQ") > 0:
                    r1 = pyod.lib.odbase_get_odr1(ther, "TLIQ", 1)
                    TLIQ[izone] = pyod.lib.odr1_get(r1, 0)

                # atmosph_comp_xco2
                if pyod.lib.odbase_card(ther, "XCO2") > 0:
                    r1 = pyod.lib.odbase_get_odr1(ther, "XCO2", 1)
                    XCO2[izone] = pyod.lib.odr1_get(r1, 0)

                # atmosph_comp_xco
                if pyod.lib.odbase_card(ther, "XCO") > 0:
                    r1 = pyod.lib.odbase_get_odr1(ther, "XCO", 1)
                    XCO[izone] = pyod.lib.odr1_get(r1, 0)

                # atmosph_comp_xh2
                if pyod.lib.odbase_card(ther, "XH2") > 0:
                    r1 = pyod.lib.odbase_get_odr1(ther, "XH2", 1)
                    XH2[izone] = pyod.lib.odr1_get(r1, 0)

                # atmosph_comp_xh2o
                if pyod.lib.odbase_card(ther, "XH2O") > 0:
                    r1 = pyod.lib.odbase_get_odr1(ther, "XH2O", 1)
                    XH2O[izone] = pyod.lib.odr1_get(r1, 0)

                # atmosph_comp_xn2
                if pyod.lib.odbase_card(ther, "XN2") > 0:
                    r1 = pyod.lib.odbase_get_odr1(ther, "XN2", 1)
                    XN2[izone] = pyod.lib.odr1_get(r1, 0)

                # atmosph_comp_xo2
                if pyod.lib.odbase_card(ther, "XO2") > 0:
                    r1 = pyod.lib.odbase_get_odr1(ther, "XO2", 1)
                    XO2[izone] = pyod.lib.odr1_get(r1, 0)
        result["average_total_pressure"] = PRES
        result["average_fluid_temperature"] = TFLU
        result["water_level"] = WLEV
        result["average_liquid_temperature"] = TLIQ
        result["atmosph_comp_xco2"] = XCO2
        result["atmosph_comp_xco"] = XCO
        result["atmosph_comp_xh2"] = XH2
        result["atmosph_comp_xh2o"] = XH2O
        result["atmosph_comp_xn2"] = XN2
        result["atmosph_comp_xo2"] = XO2

        # Fp of different form in DOME
        fp_names = ["Xe", "I", "Cs", "Te", "Sr", "Ru", "La", "Cm", "Pu", "Mo", "Sb"]
        phases = ["AEROSOL", "DEPOAREO", "GAS", "LIQUID"]
        cont = pyod.lib.odbase_get_odbase(root, "CONTAINM", 1)
        dome = pyod.lib.odbase_get_odbase(cont, "ZONE", 10)
        if pyod.lib.odbase_card(dome, "FPSM_STA") > 0:
            fpsm_sta = pyod.lib.odbase_get_odbase(dome, "FPSM_STA", 1)
            for phase_name in phases:
                phase = pyod.lib.odbase_get_odrg(fpsm_sta, phase_name, 1)
                for fp in fp_names:
                    value = pyod.lib.odrg_get(phase, fp)
                    result["m_" + fp_anonymization[fp] + "_" + phase_name.lower() + "_dome"] = value
        else:
            for phase_name in phases:
                for fp in fp_names:
                    result["m_" + fp_anonymization[fp] + "_" + phase_name.lower() + "_dome"] = np.nan

        #Possible value anonymization
        if value_anonymization:
            result = {k: result[k] / (2. ** np.array(value_anonymization[k])) for k in result}

        # time_points
        value = pyod.lib.odbase_get_double(root, "LOADTIME", 1)
        result["time_points"] = value

        return result
