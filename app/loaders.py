import hashlib
import logging
import os
import pandas as pd

from datetime import datetime, timedelta
from typing import Dict

from app.extractors import (
    ClientInfo,
    Communication,
    CustomTracker,
    DiaryEntry,
    Notification,
    PlannedEventCompletion,
    TherapySession,
    ThoughtRecord,
    SMQ
)
from app.settings import app_settings as settings
from app.transformators import (
    communications_to_treatment_snapshots,
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
    CODE_CASE_CREATED_AT = 'case_created_at'
    CODE_CLIENT_ID = 'client_id'
    CODE_TREATMENT_PHASE = 'p'
    CODE_CRITERION_A__BY_CALL = 'a__by_call'
    CODE_CRITERION_A__BY_CHAT = 'a__by_chat'
    CODE_CRITERION_B = 'b'
    CODE_CRITERION_C = 'c'
    CODE_CRITERION_D = 'd'
    CODE_CRITERION_E = 'e'
    CODE_CRITERION_F__IS_SCHEDULED = 'f__is_scheduled'
    CODE_CRITERION_F__COMPLETION_STATUS = 'f__completion_status'
    CODE_CRITERION_G__IS_REMINDER_ACTIVATED = 'g__is_reminder_activated'
    CODE_CRITERION_G__IS_COMPLETED = 'g__is_completed'
    CODE_CRITERION_H = 'h'
    CODE_CRITERION_H__LOW_SCORE = 'h__low_score'
    CODE_CRITERION_I__IS_REMINDER_ACTIVATED = 'i__is_reminder_activated'
    CODE_CRITERION_I__IS_COMPLETED = 'i__is_completed'

    def __init__(self) -> None:
        self.clients = ClientInfo().read_snapshot()
        self.communications = Communication().read_snapshot()
        self.custom_trackers = CustomTracker().read_snapshot()
        self.diary_entries = DiaryEntry().read_snapshot()
        self.notifications = Notification().read_snapshot()
        self.events_completions = PlannedEventCompletion().read_snapshot()
        self.sessions = TherapySession().read_snapshot()
        self.thought_records = ThoughtRecord().read_snapshot()
        self.smqs = SMQ().read_snapshot()

    def load(self) -> None:
        """
        Creates criteria data of the clients who has social anxiety disorder,
        and then stores them either to remote database or local storage.
        """
        criteria = self._create()

        # Clean up criteria from duplicated rows and null values
        criteria = criteria.drop_duplicates().dropna()

        self._store(criteria)

    def _create(self) -> pd.DataFrame:
        """
        Creates criteria data of the clients who has social anxiety disorder.
        """
        criteria_data = {
            Criteria.CODE_CASE_ID: [],
            Criteria.CODE_CASE_CREATED_AT: [],
            Criteria.CODE_CLIENT_ID: [],
            Criteria.CODE_TREATMENT_PHASE: [],
            Criteria.CODE_CRITERION_A__BY_CALL: [],
            Criteria.CODE_CRITERION_A__BY_CHAT: [],
            Criteria.CODE_CRITERION_B: [],
            Criteria.CODE_CRITERION_C: [],
            Criteria.CODE_CRITERION_D: [],
            Criteria.CODE_CRITERION_E: [],
            Criteria.CODE_CRITERION_F__IS_SCHEDULED: [],
            Criteria.CODE_CRITERION_F__COMPLETION_STATUS: [],
            Criteria.CODE_CRITERION_G__IS_REMINDER_ACTIVATED: [],
            Criteria.CODE_CRITERION_G__IS_COMPLETED: [],
            Criteria.CODE_CRITERION_H: [],
            Criteria.CODE_CRITERION_H__LOW_SCORE: [],
            Criteria.CODE_CRITERION_I__IS_REMINDER_ACTIVATED: [],
            Criteria.CODE_CRITERION_I__IS_COMPLETED: []
        }

        snapshots = communications_to_treatment_snapshots(self.clients, self.communications)

        for snapshot in snapshots:
            client_info = snapshot['client_info']
            timestamp = snapshot['treatment_timestamp']

            self._add_common_information(snapshot, criteria_data)
            self._add_days_since_last_contact(client_info, criteria_data, timestamp)
            self._add_days_since_last_registration(client_info, criteria_data, timestamp)
            self._add_total_registrations_of_custom_tracker(client_info, criteria_data, timestamp)
            self._add_rate_of_change_neg_regs(client_info, criteria_data, timestamp)
            self._add_rate_of_change_pos_regs(client_info, criteria_data, timestamp)
            self._add_completion_of_planned_events(client_info, criteria_data, timestamp)
            self._add_completion_of_thought_records(client_info, criteria_data, timestamp)
            self._add_smq_answers(client_info, criteria_data, timestamp)
            self._add_completion_of_diary_entries(client_info, criteria_data, timestamp)

        return pd.DataFrame(criteria_data)

    def _store(self, criteria: pd.DataFrame) -> None:
        """
        Stores criteria data to remote database / local storage.
        """
        directory, filename = FILE_LOCATOR.criteria

        running_date = str(settings.running_date())
        directory = f"{directory}/{running_date.replace('/', '-')}"

        # Create directories if they don't exists locally
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Relevant columns
        relevant_columns = [
            Criteria.CODE_CASE_ID,
            Criteria.CODE_CASE_CREATED_AT,
            Criteria.CODE_CLIENT_ID,
            Criteria.CODE_CRITERION_A__BY_CALL,
            Criteria.CODE_CRITERION_A__BY_CHAT,
            Criteria.CODE_CRITERION_B,
            Criteria.CODE_CRITERION_C,
            Criteria.CODE_CRITERION_D,
            Criteria.CODE_CRITERION_E,
            Criteria.CODE_CRITERION_F__IS_SCHEDULED,
            Criteria.CODE_CRITERION_F__COMPLETION_STATUS,
            Criteria.CODE_CRITERION_G__IS_REMINDER_ACTIVATED,
            Criteria.CODE_CRITERION_G__IS_COMPLETED,
            Criteria.CODE_CRITERION_H,
            Criteria.CODE_CRITERION_H__LOW_SCORE,
            Criteria.CODE_CRITERION_I__IS_REMINDER_ACTIVATED,
            Criteria.CODE_CRITERION_I__IS_COMPLETED,
        ]

        # Stores raw criteria dataset
        criteria[relevant_columns].to_csv(
            f"{directory}/all_{filename}",
            float_format='%g',
            index=False
        )

        # Stores criteria dataset for valid treatments
        valid_criteria = criteria.groupby('client_id').filter(self._valid_treatments)
        valid_criteria[relevant_columns].to_csv(
            f"{directory}/valid_{filename}",
            float_format='%g',
            index=False
        )

        # Stores criteria dataset with identified treatment phase
        relevant_columns.insert(2, Criteria.CODE_TREATMENT_PHASE)
        valid_criteria[relevant_columns].to_csv(
            f"{directory}/identified_valid_{filename}",
            float_format='%g',
            index=False
        )

    def _add_common_information(self, snapshot: Dict, data: Dict) -> None:
        """
        Add the common information from that `snapshot` to the criteria data.

        The common information consists of Case ID, Case Created At, Client ID,
        and their phase of the treatment.
        """
        client = snapshot['client_info']
        treatment_phase = snapshot['treatment_phase']
        treatment_timestamp = snapshot['treatment_timestamp']

        client_id = client['client_id']

        logger.info(f"Add the {client_id} common information to the criteria data...")

        # Append Case ID
        new_case_id = self._compute_case_id(client_id, client['therapist_id'], treatment_timestamp)
        data[Criteria.CODE_CASE_ID].append(new_case_id)

        # Append Snapshot's Timestamp
        data[Criteria.CODE_CASE_CREATED_AT].append(treatment_timestamp.strftime("%Y-%m-%d"))

        # Append Client ID
        data[Criteria.CODE_CLIENT_ID].append(client_id)

        # Append treatment phase
        data[Criteria.CODE_TREATMENT_PHASE].append(treatment_phase)

    def _add_days_since_last_contact(self, client: pd.Series, data: Dict, snapshot_timestamp: datetime) -> None:
        """
        Add the number of days since the last interaction
        between that `client` and their therapists to the criteria `a__by_call` and `a__by_chat`.
        """
        client_id = client['client_id']

        logger.info(f"Add {client_id} number of days since last contact to the criteria data...")

        # Filters communication data.
        communications = self.communications[
            (self.communications['client_id'] == client_id) &
            (self.communications['start_time'] <= snapshot_timestamp)
        ]

        # Filters session data.
        sessions = self.sessions[
            (self.sessions['client_id'] == client_id) &
            (self.sessions['start_time'] <= snapshot_timestamp)
        ]

        # Append criterion `a`
        a__by_call, a__by_chat = interactions_to_criterion(communications, sessions, snapshot_timestamp)
        data[Criteria.CODE_CRITERION_A__BY_CALL].append(a__by_call)
        data[Criteria.CODE_CRITERION_A__BY_CHAT].append(a__by_chat)

    def _add_days_since_last_registration(self, client: pd.Series, data: Dict, snapshot_timestamp: datetime) -> None:
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
        client_id = client['client_id']

        logger.info(f"Add {client_id} number of days since last registration to the criteria data...")

        # Filters diary entries data.
        diaries = self.diary_entries[
            (self.diary_entries['client_id'] == client_id) &
            (self.diary_entries['start_time'] <= snapshot_timestamp)
        ]

        # Filters thought records data.
        thought_records = self.thought_records[
            (self.thought_records['client_id'] == client_id) &
            (self.thought_records['start_time'] <= snapshot_timestamp)
        ]

        # Filters sessions data.
        smqs = self.smqs[
            (self.smqs['client_id'] == client_id) &
            (self.smqs['start_time'] <= snapshot_timestamp)
        ]

        # Filters custom trackers data.
        custom_trackers = self.custom_trackers[
            (self.custom_trackers['client_id'] == client_id) &
            (self.custom_trackers['start_time'] <= snapshot_timestamp)
        ]

        # Append criterion `b`
        data[Criteria.CODE_CRITERION_B].append(
            registrations_to_criterion(diaries, thought_records, smqs, custom_trackers, snapshot_timestamp)
        )

    def _add_total_registrations_of_custom_tracker(self, client: pd.Series, data: Dict, snapshot_timestamp: datetime) -> None:
        """
        Add the number of the custom tracker registrations in the last 7 days
        that have been submitted by that `client` to the criteria data.
        """
        client_id = client['client_id']

        logger.info(f"Add {client_id} total registrations of the custom trackers to the criteria data...")

        # Filters custom trackers data in the last seven days (1-7)
        from_datetime = datetime.combine(snapshot_timestamp - timedelta(days=7), datetime.max.time())
        to_datetime = datetime.combine(snapshot_timestamp, datetime.max.time())

        custom_trackers = self.custom_trackers[
            (self.custom_trackers['client_id'] == client_id) &
            (self.custom_trackers['start_time'] > from_datetime) &
            (self.custom_trackers['start_time'] <= to_datetime)
        ]

        # Append criterion `c`
        total_registrations = len(custom_trackers.index)
        data[Criteria.CODE_CRITERION_C].append(total_registrations)

    def _add_rate_of_change_neg_regs(self, client: pd.Series, data: Dict, snapshot_timestamp: datetime) -> None:
        """
        Add the comparison result of the total negative registrations made by that `client`
        between the last seven days (1-7) and the seven days before that (8-14)
        to the criteria data.
        """
        client_id = client['client_id']

        logger.info(f"Add {client_id} rate of change of the negative registrations to the criteria data...")

        # Filters custom trackers data from the last seven days (days 1-7)
        from_datetime = datetime.combine(snapshot_timestamp - timedelta(days=7), datetime.max.time())
        to_datetime = datetime.combine(snapshot_timestamp, datetime.max.time())

        trackers_past_7d = self.custom_trackers[
            (self.custom_trackers['client_id'] == client_id) &
            (self.custom_trackers['start_time'] > from_datetime) &
            (self.custom_trackers['start_time'] <= to_datetime)
        ]

        # Filters custom trackers data from one week before the last seven days (days 8-14)
        from_datetime = datetime.combine(snapshot_timestamp - timedelta(days=14), datetime.max.time())
        to_datetime = datetime.combine(snapshot_timestamp - timedelta(days=7), datetime.max.time())

        trackers_1w_before_past_7d = self.custom_trackers[
            (self.custom_trackers['client_id'] == client_id) &
            (self.custom_trackers['start_time'] > from_datetime) &
            (self.custom_trackers['start_time'] <= to_datetime)
        ]

        # Append criterion `d`
        data[Criteria.CODE_CRITERION_D].append(
            negative_registrations_to_criterion(trackers_past_7d, trackers_1w_before_past_7d)
        )

    def _add_rate_of_change_pos_regs(self, client: pd.Series, data: Dict, snapshot_timestamp: datetime) -> None:
        """
        Add the comparison result of the total positive registrations made by that `client`
        between the last seven days (1-7) and the seven days before that (8-14)
        to the criteria data.
        """
        client_id = client['client_id']

        logger.info(f"Add {client_id} rate of change of the positive registrations to the criteria data...")

        # Filters custom trackers data from the last seven days (days 1-7)
        from_datetime = datetime.combine(snapshot_timestamp - timedelta(days=7), datetime.max.time())
        to_datetime = datetime.combine(snapshot_timestamp, datetime.max.time())

        trackers_past_7d = self.custom_trackers[
            (self.custom_trackers['client_id'] == client_id) &
            (self.custom_trackers['start_time'] > from_datetime) &
            (self.custom_trackers['start_time'] <= to_datetime)
        ]

        # Filters custom trackers data from one week before the last seven days (days 8-14)
        from_datetime = datetime.combine(snapshot_timestamp - timedelta(days=14), datetime.max.time())
        to_datetime = datetime.combine(snapshot_timestamp - timedelta(days=7), datetime.max.time())

        trackers_1w_before_past_7d = self.custom_trackers[
            (self.custom_trackers['client_id'] == client_id) &
            (self.custom_trackers['start_time'] > from_datetime) &
            (self.custom_trackers['start_time'] <= to_datetime)
        ]

        # Append criterion `e`
        data[Criteria.CODE_CRITERION_E].append(
            positive_registrations_to_criterion(trackers_past_7d, trackers_1w_before_past_7d)
        )

    def _add_completion_of_planned_events(self, client: pd.Series, data: Dict, snapshot_timestamp: datetime) -> None:
        """
        Add the completion status of the `client`'s planned events to the criteria data.
        """
        client_id = client['client_id']

        logger.info(f"Add the completion status of the {client_id} planned events to the criteria data...")

        # Filters planned event's completions in the last seven days (1-7)
        from_datetime = datetime.combine(snapshot_timestamp - timedelta(days=7), datetime.max.time())
        to_datetime = datetime.combine(snapshot_timestamp, datetime.max.time())

        events = self.events_completions[
            (self.events_completions['client_id'] == client_id) &
            (self.events_completions['start_time'] > from_datetime) &
            (self.events_completions['start_time'] <= to_datetime)
        ]

        # Append criterion `f`
        schedule_priority, completion_priority = planned_events_to_criterion(events)
        data[Criteria.CODE_CRITERION_F__IS_SCHEDULED].append(schedule_priority)
        data[Criteria.CODE_CRITERION_F__COMPLETION_STATUS].append(completion_priority)

    def _add_completion_of_thought_records(self, client: pd.Series, data: Dict, snapshot_timestamp: datetime) -> None:
        """
        Add the completion status of the `client`'s thought records to the criteria data.
        """
        client_id = client['client_id']

        logger.info(f"Add the completion status of the {client_id} thought records to the criteria data...")

        # Filters thought records and theirs notification in the last seven days (1-7)
        from_datetime = datetime.combine(snapshot_timestamp - timedelta(days=7), datetime.max.time())
        to_datetime = datetime.combine(snapshot_timestamp, datetime.max.time())

        # Filters thought records data.
        thought_records = self.thought_records[
            (self.thought_records['client_id'] == client_id) &
            (self.thought_records['start_time'] > from_datetime) &
            (self.thought_records['start_time'] <= to_datetime)
        ]

        # Filters notifications data.
        notifications = self.notifications[
            (self.notifications['client_id'] == client_id) &
            (self.notifications['type'] == 'gscheme_log') &
            (self.diary_entries['start_time'] > from_datetime) &
            (self.diary_entries['start_time'] <= to_datetime)
        ]

        # Append criterion `g`
        reminder_priority, completion_priority = thought_records_to_criterion(
            thought_records, notifications)
        data[Criteria.CODE_CRITERION_G__IS_REMINDER_ACTIVATED].append(reminder_priority)
        data[Criteria.CODE_CRITERION_G__IS_COMPLETED].append(completion_priority)

    def _add_smq_answers(self, client: pd.Series, data: Dict, snapshot_timestamp: datetime) -> None:
        """
        Add the `client`'s answers of the Session Measurement Questionnaires (SMQ) to the criteria data.
        """
        client_id = client['client_id']

        logger.info(f"Add the answers of the {client_id} SMQs to the criteria data...")

        # Filters thought records data and sort them in descending order.
        smqs = self.smqs[
            (self.smqs['client_id'] == client_id) &
            (self.smqs['start_time'] <= snapshot_timestamp)
        ]
        smqs = smqs.sort_values(by=['start_time'], ascending=False)

        # Get the last two SMQs
        last_smq = smqs.iloc[0] if len(smqs.index) > 0 else None
        prev_smq = smqs.iloc[1] if len(smqs.index) > 1 else None

        # Append criterion `h`
        h__scores_diff, h__low_score = smqs_to_criterion(last_smq, prev_smq)
        data[Criteria.CODE_CRITERION_H].append(h__scores_diff)
        data[Criteria.CODE_CRITERION_H__LOW_SCORE].append(h__low_score)

    def _add_completion_of_diary_entries(self, client: pd.Series, data: Dict, snapshot_timestamp: datetime) -> None:
        """
        Add the completion status of the `client`'s diary entries to the criteria data.
        """
        client_id = client['client_id']

        logger.info(f"Add the completion status of the {client_id} diary entries to the criteria data...")

        # Filters thought records and theirs notification in the last seven days (1-7)
        from_datetime = datetime.combine(snapshot_timestamp - timedelta(days=7), datetime.max.time())
        to_datetime = datetime.combine(snapshot_timestamp, datetime.max.time())

        # Filters thought records data.
        diary_entries = self.diary_entries[
            (self.diary_entries['client_id'] == client_id) &
            (self.diary_entries['start_time'] > from_datetime) &
            (self.diary_entries['start_time'] <= to_datetime)
        ]

        # Filters notifications data.
        notifications = self.notifications[
            (self.notifications['client_id'] == client_id) &
            (self.notifications['type'] == 'diary_entry_log') &
            (self.diary_entries['start_time'] > from_datetime) &
            (self.diary_entries['start_time'] <= to_datetime)
        ]

        # Append criterion `i`
        reminder_priority, completion_priority = diary_entries_to_criterion(
            diary_entries, notifications)
        data[Criteria.CODE_CRITERION_I__IS_REMINDER_ACTIVATED].append(reminder_priority)
        data[Criteria.CODE_CRITERION_I__IS_COMPLETED].append(completion_priority)

    def _valid_treatments(self, group: any) -> any:
        """
        Returns criteria condition of valid treatments.
        """
        condition = (
            (
                # Days since last contact
                (
                    (group[Criteria.CODE_CRITERION_A__BY_CALL].max() <= 30) |
                    (group[Criteria.CODE_CRITERION_A__BY_CHAT].max() <= 30)
                ) and
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
