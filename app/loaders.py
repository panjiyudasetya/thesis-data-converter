import hashlib
import logging
import os
import pandas as pd
import random

from datetime import date, datetime, timedelta
from typing import Dict, List

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
from app.settings import app_settings as settings
from app.transformators import (
    diary_entries_to_criterion,
    interactions_to_criterion,
    negative_registrations_to_criterion,
    planned_events_to_criterion,
    positive_registrations_to_criterion,
    registrations_to_criterion,
    smqs_to_criterion,
    thought_records_to_criterion,
)


logger = logging.getLogger(__name__)
FILE_LOCATOR = settings.FILE_LOCATOR


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

    def __init__(self) -> None:
        # Select dataset for the specific date
        self.clients = ClientInfo().read_snapshot()
        self.communications = Communication().read_snapshot()
        self.custom_trackers = CustomTracker().read_snapshot()
        self.diary_entries = DiaryEntry().read_snapshot()
        self.notifications = Notification().read_snapshot()
        self.events = PlannedEvent().read_snapshot()
        self.events_reflections = PlannedEventReflection().read_snapshot()
        self.sessions = TherapySession().read_snapshot()
        self.thought_records = ThoughtRecord().read_snapshot()
        self.smqs = SMQ().read_snapshot()

    def load(self) -> None:
        """
        Creates criteria data of the clients who has social anxiety disorder,
        and then stores them either to remote database or local storage.
        """
        criteria = self._create()
        self._store(criteria)

    def _create(self) -> pd.DataFrame:
        """
        Creates criteria data of the clients who has social anxiety disorder.
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
            Criteria.CODE_CRITERION_I: []
        }

        for _, client in self.clients.iterrows():

            # Get client's snapshots.
            client_id = client['client_id']
            first_session_timestamp = client['start_time']
            snapshots = self._get_client_snapshots(client_id, first_session_timestamp)

            # Generate client's criteria.
            for treatment_phase, timestamp in snapshots:

                client_info = {
                    'client_id': client_id,
                    'therapist_id': client['therapist_id'],
                    'phase': treatment_phase,
                    'timestamp': timestamp
                }

                self._add_common_information(client_info, criteria_data)
                self._add_days_since_last_contact(client_info, criteria_data)
                self._add_days_since_last_registration(client_info, criteria_data)
                self._add_total_registrations_of_custom_tracker(client_info, criteria_data)
                self._add_rate_of_change_neg_regs(client_info, criteria_data)
                self._add_rate_of_change_pos_regs(client_info, criteria_data)
                self._add_completion_of_planned_events(client_info, criteria_data)
                self._add_completion_of_thought_records(client_info, criteria_data)
                self._add_smq_answers(client_info, criteria_data)
                self._add_completion_of_diary_entries(client_info, criteria_data)

        return pd.DataFrame(criteria_data)

    def _store(self, criteria: pd.DataFrame) -> None:
        """
        Stores criteria data to remote database / local storage.
        """
        running_date = str(settings.running_date())
        directory = f"{FILE_LOCATOR.criteria[FILE_LOCATOR.DIR]}/{running_date.replace('/', '-')}"
        filename = FILE_LOCATOR.criteria[FILE_LOCATOR.FILENAME]

        # Create directories if they don't exists locally
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Stores raw criteria dataset
        criteria.to_csv(f"{directory}/all_{filename}", float_format='%g', index=False)

        # Stores criteria dataset for valid treatments
        valid_criteria = criteria.groupby('client_id').filter(self._valid_treatments)
        valid_criteria.to_csv(f"{directory}/valid_{filename}", float_format='%g', index=False)

    def _add_common_information(self, client: Dict, data: Dict) -> None:
        """
        Add the common information of that `client` to the criteria data.

        The common information consists of Case ID, Client ID, and his phase of the treatment.
        """
        logger.info(f"Add the {client['client_id']} common information to the criteria data...")

        client_id = client['client_id']

        # Append Case ID
        new_case_id = self._compute_case_id(client_id, client['therapist_id'], client['timestamp'])
        data[Criteria.CODE_CASE_ID].append(new_case_id)

        # Append Client ID
        data[Criteria.CODE_CLIENT_ID].append(client_id)

        # Append treatment phase
        data[Criteria.CODE_TREATMENT_PHASE].append(client['phase'])

    def _add_days_since_last_contact(self, client: Dict, data: Dict) -> None:
        """
        Add the number of days since the last interaction
        between that `client` and their therapists to the criteria data.

        The last interaction date defined as the maximum date between:
        - The date of the last therapy session; AND
        - The date of their last chat interaction; AND
        - The date of their last call interaction;
        """
        logger.info(f"Add {client['client_id']} number of days since last contact to the criteria data...")

        client_id = client['client_id']
        timestamp = client['timestamp']

        # Filters communication data.
        communications = self.communications[
            (self.communications['client_id'] == client_id) &
            (self.communications['start_time'] <= timestamp)
        ]

        # Filters session data.
        sessions = self.sessions[
            (self.sessions['client_id'] == client_id) &
            (self.sessions['start_time'] <= timestamp)
        ]

        # Append criterion `a`
        data[Criteria.CODE_CRITERION_A].append(
            interactions_to_criterion(communications, sessions, timestamp)
        )

    def _add_days_since_last_registration(self, client: Dict, data: Dict) -> None:
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

        client_id = client['client_id']
        timestamp = client['timestamp']

        # Filters diary entries data.
        diaries = self.diary_entries[
            (self.diary_entries['client_id'] == client_id) &
            (self.diary_entries['start_time'] <= timestamp)
        ]

        # Filters thought records data.
        thought_records = self.thought_records[
            (self.thought_records['client_id'] == client_id) &
            (self.thought_records['start_time'] <= timestamp)
        ]

        # Filters sessions data.
        smqs = self.smqs[
            (self.smqs['client_id'] == client_id) &
            (self.smqs['start_time'] <= timestamp)
        ]

        # Filters custom trackers data.
        custom_trackers = self.custom_trackers[
            (self.custom_trackers['client_id'] == client_id) &
            (self.custom_trackers['start_time'] <= timestamp)
        ]

        # Append criterion `b`
        data[Criteria.CODE_CRITERION_B].append(
            registrations_to_criterion(diaries, thought_records, smqs, custom_trackers, timestamp)
        )

    def _add_total_registrations_of_custom_tracker(self, client: Dict, data: Dict) -> None:
        """
        Add the number of the custom tracker registrations in the last 7 days
        that have been submitted by that `client` to the criteria data.
        """
        logger.info(f"Add {client['client_id']} total registrations of the custom trackers to the criteria data...")

        client_id = client['client_id']
        timestamp = client['timestamp']

        # Filters custom trackers data in the last seven days (1-7)
        from_datetime = datetime.combine(timestamp - timedelta(days=7), datetime.max.time())
        to_datetime = datetime.combine(timestamp, datetime.max.time())

        custom_trackers = self.custom_trackers[
            (self.custom_trackers['client_id'] == client_id) &
            (self.custom_trackers['start_time'] > from_datetime) &
            (self.custom_trackers['start_time'] <= to_datetime)
        ]

        # Append criterion `c`
        total_registrations = len(custom_trackers.index)
        data[Criteria.CODE_CRITERION_C].append(total_registrations)

    def _add_rate_of_change_neg_regs(self, client: Dict, data: Dict) -> None:
        """
        Add the comparison result of the total negative registrations made by that `client`
        between the last seven days (1-7) and the seven days before that (8-14)
        to the criteria data.
        """
        logger.info(f"Add {client['client_id']} rate of change of the negative registrations to the criteria data...")

        client_id = client['client_id']
        timestamp = client['timestamp']

        # Filters custom trackers data from the last seven days (days 1-7)
        from_datetime = datetime.combine(timestamp - timedelta(days=7), datetime.max.time())
        to_datetime = datetime.combine(timestamp, datetime.max.time())

        trackers_past_7d = self.custom_trackers[
            (self.custom_trackers['client_id'] == client_id) &
            (self.custom_trackers['start_time'] > from_datetime) &
            (self.custom_trackers['start_time'] <= to_datetime)
        ]

        # Filters custom trackers data from one week before the last seven days (days 8-14)
        from_datetime = datetime.combine(timestamp - timedelta(days=14), datetime.max.time())
        to_datetime = datetime.combine(timestamp - timedelta(days=7), datetime.max.time())

        trackers_1w_before_past_7d = self.custom_trackers[
            (self.custom_trackers['client_id'] == client_id) &
            (self.custom_trackers['start_time'] > from_datetime) &
            (self.custom_trackers['start_time'] <= to_datetime)
        ]

        # Append criterion `d`
        data[Criteria.CODE_CRITERION_D].append(
            negative_registrations_to_criterion(trackers_past_7d, trackers_1w_before_past_7d)
        )

    def _add_rate_of_change_pos_regs(self, client: Dict, data: Dict) -> None:
        """
        Add the comparison result of the total positive registrations made by that `client`
        between the last seven days (1-7) and the seven days before that (8-14)
        to the criteria data.
        """
        logger.info(f"Add {client['client_id']} rate of change of the positive registrations to the criteria data...")

        client_id = client['client_id']
        timestamp = client['timestamp']

        # Filters custom trackers data from the last seven days (days 1-7)
        from_datetime = datetime.combine(timestamp - timedelta(days=7), datetime.max.time())
        to_datetime = datetime.combine(timestamp, datetime.max.time())

        trackers_past_7d = self.custom_trackers[
            (self.custom_trackers['client_id'] == client_id) &
            (self.custom_trackers['start_time'] > from_datetime) &
            (self.custom_trackers['start_time'] <= to_datetime)
        ]

        # Filters custom trackers data from one week before the last seven days (days 8-14)
        from_datetime = datetime.combine(timestamp - timedelta(days=14), datetime.max.time())
        to_datetime = datetime.combine(timestamp - timedelta(days=7), datetime.max.time())

        trackers_1w_before_past_7d = self.custom_trackers[
            (self.custom_trackers['client_id'] == client_id) &
            (self.custom_trackers['start_time'] > from_datetime) &
            (self.custom_trackers['start_time'] <= to_datetime)
        ]

        # Append criterion `e`
        data[Criteria.CODE_CRITERION_E].append(
            positive_registrations_to_criterion(trackers_past_7d, trackers_1w_before_past_7d)
        )

    def _add_completion_of_planned_events(self, client: Dict, data: Dict) -> None:
        """
        Add the completion status of the `client`'s planned events to the criteria data.
        """
        logger.info(f"Add the completion status of the {client['client_id']} planned events to the criteria data...")

        client_id = client['client_id']
        timestamp = client['timestamp']

        # Filters events and their reflections in the last seven days (1-7)
        from_datetime = datetime.combine(timestamp - timedelta(days=7), datetime.max.time())
        to_datetime = datetime.combine(timestamp, datetime.max.time())

        # Filters planned events data.
        events = self.events[
            (self.events['client_id'] == client_id) &
            (self.events['start_time'] > from_datetime) &
            (self.events['start_time'] <= to_datetime)
        ]

        # Filters event's reflections data.
        events_reflections = self.events_reflections[
            self.events_reflections['planned_event_id'].isin(events['id'].to_list())
        ]
        events_reflections = events_reflections[
            (events_reflections['start_time'] > from_datetime) &
            (events_reflections['start_time'] <= to_datetime)
        ]

        # Append criterion `f`
        data[Criteria.CODE_CRITERION_F].append(
            planned_events_to_criterion(events, events_reflections, from_datetime, to_datetime)
        )

        return data

    def _add_completion_of_thought_records(self, client: Dict, data: Dict) -> None:
        """
        Add the completion status of the `client`'s thought records to the criteria data.
        """
        logger.info(f"Add the completion status of the {client['client_id']} thought records to the criteria data...")

        client_id = client['client_id']
        timestamp = client['timestamp']

        # Filters thought records and theirs notification in the last seven days (1-7)
        from_datetime = datetime.combine(timestamp - timedelta(days=7), datetime.max.time())
        to_datetime = datetime.combine(timestamp, datetime.max.time())

        # Filters thought records data.
        thought_records = self.thought_records[
            (self.thought_records['client_id'] == client_id) &
            (self.thought_records['start_time'] > from_datetime) &
            (self.thought_records['start_time'] <= to_datetime)
        ]

        # Filters notifications data.
        notifications = self.notifications[
            (self.notifications['client_id'] == client_id) &
            (self.notifications['type'] == 'gscheme_log')
        ]

        # Append criterion `g`
        data[Criteria.CODE_CRITERION_G].append(
            thought_records_to_criterion(thought_records, notifications)
        )

    def _add_smq_answers(self, client: Dict, data: Dict) -> None:
        """
        Add the `client`'s answers of the Session Measurement Questionnaires (SMQ) to the criteria data.
        """
        logger.info(f"Add the answers of the {client['client_id']} SMQs to the criteria data...")

        client_id = client['client_id']

        # Filters thought records data and sort them in descending order.
        smqs = self.smqs[(self.smqs['client_id'] == client_id)]
        smqs = smqs.sort_values(by=['start_time'], ascending=False)

        # Get the last two SMQs
        last_smq = smqs.iloc[0] if len(smqs.index) > 0 else None
        prev_smq = smqs.iloc[1] if len(smqs.index) > 1 else None

        # Append criterion `h`
        data[Criteria.CODE_CRITERION_H].append(smqs_to_criterion(last_smq, prev_smq))

    def _add_completion_of_diary_entries(self, client: Dict, data: Dict) -> None:
        """
        Add the completion status of the `client`'s diary entries to the criteria data.
        """
        logger.info(f"Add the completion status of the {client['client_id']} diary entries to the criteria data...")

        client_id = client['client_id']
        timestamp = client['timestamp']

        # Filters thought records and theirs notification in the last seven days (1-7)
        from_datetime = datetime.combine(timestamp - timedelta(days=7), datetime.max.time())
        to_datetime = datetime.combine(timestamp, datetime.max.time())

        # Filters thought records data.
        diary_entries = self.diary_entries[
            (self.diary_entries['client_id'] == client_id) &
            (self.diary_entries['start_time'] > from_datetime) &
            (self.diary_entries['start_time'] <= to_datetime)
        ]

        # Filters notifications data.
        notifications = self.notifications[
            (self.notifications['client_id'] == client_id) &
            (self.notifications['type'] == 'diary_entry_log')
        ]

        # Append criterion `i`
        data[Criteria.CODE_CRITERION_I].append(
            diary_entries_to_criterion(diary_entries, notifications)
        )

    def _get_client_snapshots(self, client_id: str, start_time: date) -> List[datetime]:
        """
        Returns list of client's snapshots.
        """
        # Filters communication data to the audio/video calls.
        calls = self.communications[
            (self.communications['client_id'] == client_id) &
            (self.communications['call_made'])
        ]

        # Sorts audio/video calls in ascending order
        call_timestamps = calls['start_time'].sort_values().iloc[:]

        # Ensures the given `start_time` is equals to the first time of the client's audio/video call
        if call_timestamps.iloc[0] != start_time:
            raise ValueError(f"Communications data error for client: {client_id}")

        # Normally, clients does an online-treatment for 13 times to feel better.
        # The first and second audio/video call aims to observe the client (intake sessions)
        # and it doesn't counts as an online-treatment.
        session_indexes = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
        snapshots = []

        # Generates snapshots

        PHASE_START = 0
        PHASE_MID = 1
        PHASE_END = 2

        for session, timestamp in enumerate(call_timestamps):
            # Validate session
            if session not in session_indexes:
                continue

            # Client is in the beginning of treatment
            if session <= 3:
                treatment_phase = PHASE_START

            # Client is in the middle of treatment
            elif session <= 8:
                treatment_phase = PHASE_MID

            # Client is in the end of treatment
            else:
                treatment_phase = PHASE_END

            snapshot = (treatment_phase, timestamp - timedelta(days=random.randint(0, 6)))
            snapshots.append(snapshot)

        return snapshots

    def _valid_treatments(self, group: any) -> any:
        """
        Returns criteria condition of valid treatments.
        """
        condition = (
            (
                # Days since last contact
                group[Criteria.CODE_CRITERION_A].max() <= 30 and
                # Days since last registration
                group[Criteria.CODE_CRITERION_B].max() <= 30 and
                # No. of. custom trackers registrations in the past 7 days
                group[Criteria.CODE_CRITERION_C].gt(0).sum() >= 2
            )
        )
        return condition

    def _compute_case_id(self, client_id: str, therapist_id: str, timestamp: datetime) -> str:
        """
        Hashes the incoming values with MD5.
        """
        plain_case_id = f"{client_id}#{therapist_id}#{str(timestamp)}"
        return hashlib.md5(plain_case_id.encode()).hexdigest()
