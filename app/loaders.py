import logging
import numpy as np
import pandas as pd

from datetime import date

from app.extractors import (
    ClientProfile,
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


logger = logging.getLogger(__name__)


class Criteria:

    def __init__(self, for_date: date) -> None:
        # Set the date of when criteria is formulated
        self.for_date = for_date

        # Select dataset for the specific date
        self.clients = ClientProfile().select_snapshot(self.for_date)
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
        criteria = pd.DataFrame()

        self._add_common_information(criteria)
        self._add_phases_of_the_treatment(criteria)
        self._add_days_since_last_contact(criteria)
        self._add_days_since_last_registration(criteria)
        self._add_total_registrations_of_custom_tracker(criteria)
        self._add_rate_of_change_neg_regs(criteria)
        self._add_rate_of_change_pos_regs(criteria)
        self._add_completion_of_planned_activities(criteria)
        self._add_completion_of_thought_records(criteria)
        self._add_smq_answers(criteria)
        self._add_completion_of_diary_entries(criteria)

        return criteria

    def _store(self, criteria) -> None:
        """
        Stores criteria data to remote database / local storage.
        """
        pass

    def _add_common_information(self, to_criteria: pd.DataFrame) -> pd.DataFrame:
        """
        Add the common information to the criteria dataframe.

        The common information consists of:
        - The list of Patient IDs
        - The dates of when the first treatment is occurred after the intake session
        """
        logger.info("Add common information to the criteria dataframe...")

        # TODO: Assign common information to the criteria dataframe

        return to_criteria

    def _add_phases_of_the_treatment(self, to_criteria: pd.DataFrame) -> pd.DataFrame:
        """
        Add phases of the treatment to the criteria dataframe.
        """
        logger.info("Add phases of the treatment to the criteria dataframe...")

        # TODO: Assign criterium to the criteria dataframe
        to_criteria['p'] = np.array([])

    def _add_days_since_last_contact(self, to_criteria: pd.DataFrame) -> pd.DataFrame:
        """
        Add the number of days since the last interaction
        between the clients and their therapists to the criteria dataframe.

        The last interaction date defined as the maximum date between:
        - The date of the last therapy session; AND
        - The date of their last chat interaction; AND
        - The date of their last call interaction;
        """
        logger.info("Add number of days since last contact to the criteria dataframe...")

        # TODO: Assign criterium to the criteria dataframe
        to_criteria['a'] = np.array([])

    def _add_days_since_last_registration(self, to_criteria: pd.DataFrame) -> pd.DataFrame:
        """
        Add the number of days since the last time the clients made a registration
        for one of these trackers:
        - Diary
        - GSchema
        - SMQ
        - Custom trackers:
            - Avoidance
            - Safety behavior
            - Worry
        """
        logger.info("Add number of days since last registration to the criteria dataframe...")

        # TODO: Assign criterium to the criteria dataframe
        to_criteria['b'] = np.array([])

    def _add_total_registrations_of_custom_tracker(self, to_criteria: pd.DataFrame) -> pd.DataFrame:
        """
        Add the number of the custom tracker registrations in the last 7 days
        that have been submitted by the clients to the criteria dataframe.
        """
        logger.info("Add total registrations of the clients' custom trackers to the criteria dataframe...")

        # TODO: Assign criterium to the criteria dataframe
        to_criteria['c'] = np.array([])

    def _add_rate_of_change_neg_regs(self, to_criteria: pd.DataFrame) -> pd.DataFrame:
        """
        Add the comparison result of the total negative registrations made by the clients
        between the last seven days (1-7) and the seven days before that (8-14)
        to the criteria dataframe.
        """
        logger.info("Add rate of change of the negative registrations to the criteria dataframe...")

        # TODO: Assign criterium to the criteria dataframe
        to_criteria['d'] = np.array([])

    def _add_rate_of_change_pos_regs(self, to_criteria: pd.DataFrame) -> pd.DataFrame:
        """
        Add the comparison result of the total positive registrations made by the patient
        between the last seven days (1-7) and the seven days before that (8-14)
        to the criteria dataframe.
        """
        logger.info("Add rate of change of the positive registrations to the criteria dataframe...")

        # TODO: Assign criterium to the criteria dataframe
        to_criteria['e'] = np.array([])

    def _add_completion_of_planned_activities(self, to_criteria: pd.DataFrame) -> pd.DataFrame:
        """
        Add the completion statuses of the planned activities to the criteria dataframe.
        """
        
        logger.info("Add rate of change of the positive registrations to the criteria dataframe...")

        # TODO: Assign criterium to the criteria dataframe
        to_criteria['f'] = np.array([])

    def _add_completion_of_thought_records(self, to_criteria: pd.DataFrame) -> pd.DataFrame:
        """
        Add the thought record's completion statuses to the criteria dataframe.
        """
        logger.info("Add the thought record's completion statuses to the criteria dataframe...")

        # TODO: Assign criterium to the criteria dataframe
        to_criteria['g'] = np.array([])

    def _add_smq_answers(self, to_criteria: pd.DataFrame) -> pd.DataFrame:
        """
        Add the answers of the Session Measurement Questionnaires (SMQ) to the criteria dataframe.
        """
        logger.info("Add the SMQ's answers to the criteria dataframe...")

        # TODO: Assign criterium to the criteria dataframe
        to_criteria['h'] = np.array([])

    def _add_completion_of_diary_entries(self, to_criteria: pd.DataFrame) -> pd.DataFrame:
        """
        Add the diary entry's completion statuses to the criteria dataframe.
        """
        logger.info("Add the diary entry's completion statuses to the criteria dataframe...")

        # TODO: Assign criterium to the criteria dataframe
        to_criteria['i'] = np.array([])
