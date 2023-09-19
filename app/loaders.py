import hashlib
import logging
import numpy as np
import pandas as pd

from datetime import date
from typing import Dict

from app.extractors import (
    ClientInfo,
    Communication,
    CustomTracker,
    DiaryEntry,
    Notification,
    PlannedEvent,
    PlannedEventReflection,
    TherapySession,
    ThoughtRecord,
    SMQ
)
from app.transformators import (
    calls_to_treatment_phase
)


logger = logging.getLogger(__name__)


class Criteria:

    CODE_CASE_ID = 'case_id'
    CODE_CLIENT_ID = 'client_id'
    CODE_TREATMENT_PHASE = 'p'
    CODE_CRITERION_A = 'a'
    CODE_CRITERION_B = 'b'
    CODE_CRITERION_C = 'c'
    CODE_CRITERION_D = 'd'
    CODE_CRITERION_E = 'e'
    CODE_CRITERION_F = 'f'
    CODE_CRITERION_G = 'g'
    CODE_CRITERION_H = 'h'
    CODE_CRITERION_I = 'i'

    def __init__(self, for_date: date) -> None:
        # Set the date of when criteria is formulated
        self.for_date = for_date

        # Select dataset for the specific date
        self.clients = ClientInfo().select_snapshot(self.for_date)
        self.communications = Communication().select_snapshot(self.for_date)
        self.custom_trackers = CustomTracker().select_snapshot(self.for_date)
        self.diary_entries = DiaryEntry().select_snapshot(self.for_date)
        self.notifications = Notification().select_snapshot(self.for_date)
        self.events = PlannedEvent().select_snapshot(self.for_date)
        self.events_reflections = PlannedEventReflection().select_snapshot(self.for_date)
        self.sessions = TherapySession().select_snapshot(self.for_date)
        self.thought_records = ThoughtRecord().select_snapshot(self.for_date)
        self.smqs = SMQ().select_snapshot(self.for_date)

    def load(self) -> None:
        """
        Creates criteria data of the clients who has social anxiety disorder
        at the given `self.for_date`, and then stores them either to remote database
        or local storage.
        """
        criteria = self._create()
        self._store(criteria)

    def _create(self) -> pd.DataFrame:
        """
        Creates criteria data of the clients who has social anxiety disorder
        at the given `self.for_date`.
        """
        criteria_data = {
            Criteria.CODE_CASE_ID: [],
            Criteria.CODE_CLIENT_ID: [],
            Criteria.CODE_TREATMENT_PHASE: [],
            Criteria.CODE_CRITERION_A: [],
            Criteria.CODE_CRITERION_B: [],
            Criteria.CODE_CRITERION_C: [],
            Criteria.CODE_CRITERION_D: [],
            Criteria.CODE_CRITERION_E: [],
            Criteria.CODE_CRITERION_F: [],
            Criteria.CODE_CRITERION_G: [],
            Criteria.CODE_CRITERION_H: [],
            Criteria.CODE_CRITERION_I: [],
        }

        for _, client_info in self.clients.iterrows():
            self._add_common_information(client_info, criteria_data)
            self._add_treatment_phase(client_info, criteria_data)
            self._add_days_since_last_contact(client_info, criteria_data)
            self._add_days_since_last_registration(client_info, criteria_data)
            self._add_total_registrations_of_custom_tracker(client_info, criteria_data)
            self._add_rate_of_change_neg_regs(client_info, criteria_data)
            self._add_rate_of_change_pos_regs(client_info, criteria_data)
            self._add_completion_of_planned_events(client_info, criteria_data)
            self._add_completion_of_thought_records(client_info, criteria_data)
            self._add_smq_answers(client_info, criteria_data)
            self._add_completion_of_diary_entries(client_info, criteria_data)

        from pdb import set_trace
        set_trace()

        return pd.DataFrame(criteria_data)

    def _store(self, criteria) -> None:
        """
        Stores criteria data to remote database / local storage.
        """
        pass

    def _add_common_information(self, client: pd.Series, data: Dict) -> None:
        """
        Add the common information of that `client` to the criteria data.

        The common information consists of:
        - Case IDs;
        - Client IDs;
        """
        logger.info(f"Add the {client['client_id']} common information to the criteria data...")

        client_id = client['client_id']

        # Append Case ID
        new_case_id = self._compute_case_id(client_id, client['therapist_id'])
        data[Criteria.CODE_CASE_ID].append(new_case_id)

        # Append Client ID
        data[Criteria.CODE_CLIENT_ID].append(client_id)

    def _add_treatment_phase(self, client: pd.Series, data: Dict) -> None:
        """
        Add phase of the treatment of that `client` to the criteria data.
        """
        logger.info(f"Add {client['client_id']} phase of the treatment to the criteria data...")

        # Filters communication data.
        calls = self.communications[
            (self.communications['client_id'] == client['client_id']) &
            (self.communications['call_made']) &
            (self.communications['start_time'] <= np.datetime64(str(self.for_date)))
        ]

        # Append treatment phase
        data[Criteria.CODE_TREATMENT_PHASE].append(calls_to_treatment_phase(calls))

    def _add_days_since_last_contact(self, client: pd.Series, data: Dict) -> None:
        """
        Add the number of days since the last interaction
        between that `client` and their therapists to the criteria data.

        The last interaction date defined as the maximum date between:
        - The date of the last therapy session; AND
        - The date of their last chat interaction; AND
        - The date of their last call interaction;
        """
        logger.info(f"Add {client['client_id']} number of days since last contact to the criteria data...")

        # TODO: Assign criterium to the criteria data
        data[Criteria.CODE_CRITERION_A].append(None)

    def _add_days_since_last_registration(self, client: pd.Series, data: Dict) -> None:
        """
        Add the number of days since the last time the `client` made a registration
        for one of these trackers:
        - Diary
        - GSchema
        - SMQ
        - Custom trackers:
            - Avoidance
            - Safety behavior
            - Worry
        """
        logger.info(f"Add {client['client_id']} number of days since last registration to the criteria data...")

        # TODO: Assign criterium to the criteria data
        data[Criteria.CODE_CRITERION_B].append(None)

    def _add_total_registrations_of_custom_tracker(self, client: pd.Series, data: Dict) -> None:
        """
        Add the number of the custom tracker registrations in the last 7 days
        that have been submitted by that `client` to the criteria data.
        """
        logger.info(f"Add {client['client_id']} total registrations of the custom trackers to the criteria data...")

        # TODO: Assign criterium to the criteria data
        data[Criteria.CODE_CRITERION_C].append(None)

    def _add_rate_of_change_neg_regs(self, client: pd.Series, data: Dict) -> None:
        """
        Add the comparison result of the total negative registrations made by that `client`
        between the last seven days (1-7) and the seven days before that (8-14)
        to the criteria data.
        """
        logger.info(f"Add {client['client_id']} rate of change of the negative registrations to the criteria data...")

        # TODO: Assign criterium to the criteria data
        data[Criteria.CODE_CRITERION_D].append(None)

    def _add_rate_of_change_pos_regs(self, client: pd.Series, data: Dict) -> None:
        """
        Add the comparison result of the total positive registrations made by that `client`
        between the last seven days (1-7) and the seven days before that (8-14)
        to the criteria data.
        """
        logger.info(f"Add {client['client_id']} rate of change of the positive registrations to the criteria data...")

        # TODO: Assign criterium to the criteria data
        data[Criteria.CODE_CRITERION_E].append(None)

    def _add_completion_of_planned_events(self, client: pd.Series, data: Dict) -> None:
        """
        Add the completion status of the `client`'s planned events to the criteria data.
        """
        
        logger.info(f"Add the completion status of the {client['client_id']} planned events to the criteria data...")

        # TODO: Assign criterium to the criteria data
        data[Criteria.CODE_CRITERION_F].append(None)

        return data

    def _add_completion_of_thought_records(self, client: pd.Series, data: Dict) -> None:
        """
        Add the completion status of the `client`'s thought records to the criteria data.
        """
        logger.info(f"Add the completion status of the {client['client_id']} thought records to the criteria data...")

        # TODO: Assign criterium to the criteria data
        data[Criteria.CODE_CRITERION_G].append(None)

    def _add_smq_answers(self, client: pd.Series, data: Dict) -> None:
        """
        Add the `client`'s answers of the Session Measurement Questionnaires (SMQ) to the criteria data.
        """
        logger.info(f"Add the answers of the {client['client_id']} SMQs to the criteria data...")

        # TODO: Assign criterium to the criteria data
        data[Criteria.CODE_CRITERION_H].append(None)

    def _add_completion_of_diary_entries(self, client: pd.Series, data: Dict) -> None:
        """
        Add the completion status of the `client`'s diary entries to the criteria data.
        """
        logger.info(f"Add the completion status of the {client['client_id']} diary entries to the criteria data...")

        # TODO: Assign criterium to the criteria data
        data[Criteria.CODE_CRITERION_I].append(None)

    def _compute_case_id(self, client_id: str, therapist_id: str) -> str:
        """
        Hashes the incoming values with MD5.
        """
        plain_case_id = f"{client_id}#{therapist_id}#{self.for_date}"
        return hashlib.md5(plain_case_id.encode()).hexdigest()
