# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
UI widget for viewing an AWS CloudWatch Logstream.
"""
from __future__ import annotations
import os
from logging import getLogger
from typing import Optional, Any
from datetime import datetime, timedelta, timezone
from bisect import bisect_left, bisect_right

from PySide2.QtCore import Qt
from PySide2.QtWidgets import (  # type: ignore
    QAbstractSlider,
    QHBoxLayout,
    QHeaderView,
    QScrollBar,
    QTableWidget,
    QTableWidgetItem,
    QWidget,
)

from .. import block_signals

logger = getLogger(__name__)


def _normalize_events(events):
    # Converts the CloudWatch Logs events from dicts to tuples
    # and splits any multi-line events into multiple separate events.
    result = []
    for event in events:
        timestamp = datetime.fromtimestamp(event["timestamp"] / 1000, tz=timezone.utc)
        message_list = event["message"].splitlines()
        for message in message_list:
            result.append((timestamp, message))
    return result


def get_log_events_forward(
    logs,
    log_group_name,
    log_stream_name,
    *,
    count,
    start_time=None,
    end_time=None,
    next_forward_token=None,
):
    """
    Returns up to the requested count of events from the log stream, forwards from start_time or the next_forward_token.
    It stops only when the count is reached or the log stream ends.

    Returns (events, next_backward_token, next_forward_token)
    """
    kwargs = {}
    if start_time is not None:
        kwargs["startTime"] = int(start_time.timestamp() * 1000)
    if end_time is not None:
        kwargs["endTime"] = int(end_time.timestamp() * 1000)
    if next_forward_token is not None:
        kwargs["nextToken"] = next_forward_token
    response = logs.get_log_events(
        logGroupName=log_group_name,
        logStreamName=log_stream_name,
        startFromHead=True,
        limit=count,
        **kwargs,
    )
    events = response["events"]
    next_backward_token = response["nextBackwardToken"]

    # Keep calling until we reach the event count, or signals the end by returning the token we gave it
    next_forward_token = None
    while len(events) < count and response["nextForwardToken"] != next_forward_token:
        next_forward_token = response["nextForwardToken"]
        response = logs.get_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            startFromHead=True,
            limit=count - len(events),
            nextToken=next_forward_token,
        )
        events.extend(response["events"])

    events = _normalize_events(events)
    return (events, next_backward_token, response["nextForwardToken"])


def get_log_events_backward(
    logs,
    log_group_name,
    log_stream_name,
    *,
    count,
    start_time=None,
    end_time=None,
    next_backward_token=None,
):
    """
    Returns up to the requested count of events from the log stream, backwards from end_time or the next_backward_token.
    It stops only when the count is reached or the log stream ends.
    """
    kwargs = {}
    if start_time is not None:
        kwargs["startTime"] = int(start_time.timestamp() * 1000)
    if end_time is not None:
        kwargs["endTime"] = int(end_time.timestamp() * 1000)
    if next_backward_token is not None:
        kwargs["nextToken"] = next_backward_token
    response = logs.get_log_events(
        logGroupName=log_group_name,
        logStreamName=log_stream_name,
        startFromHead=False,
        limit=count,
        **kwargs,
    )
    events_list = [response["events"]]
    event_count = len(events_list[0])
    next_forward_token = response["nextForwardToken"]

    next_backward_token = None
    # Keep calling until we reach the event count, or signals the end by returning the token we gave it
    while event_count < count and response["nextBackwardToken"] != next_backward_token:
        next_backward_token = response["nextBackwardToken"]
        response = logs.get_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            startFromHead=False,
            limit=count - event_count,
            nextToken=next_backward_token,
        )
        if response["events"]:
            events_list.append(response["events"])
            event_count += len(response["events"])

    events = []
    for events_entry in reversed(events_list):
        events.extend(_normalize_events(events_entry))
    return (events, response["nextBackwardToken"], next_forward_token)


class SlidingLogPortholeQuery:
    # These are the backward and forward tokens from the CloudWatch Logs get_log_events call(s).
    next_backward_token: Optional[str]
    next_forward_token: Optional[str]
    # This is a half-open interval of indexes in the SlidingLogPorthole events list.
    # The indexes are range(events_start_index, events_end_index).
    events_start_index: int
    events_end_index: int

    def __init__(
        self, next_backward_token, next_forward_token, events_start_index, events_end_index
    ):
        self.next_backward_token = next_backward_token
        self.next_forward_token = next_forward_token
        self.events_start_index = events_start_index
        self.events_end_index = events_end_index


class SlidingLogPorthole:
    # A list of tuples (timestamp, one_log_line)
    events: list[tuple]
    # The index into events, of the top of the porthole
    position: int
    # We maintain the backward/forward tokens and index range of every query
    # that we've made while sliding. This is important to be able to discard
    # values from either end due to memory constraints, while keeping the ability
    # to extend in that direction again.
    nav_tokens: list[SlidingLogPortholeQuery]

    # These are True when we reached the beginning/end of the log, respectively
    contains_head: bool
    contains_tail: bool

    # Properties for CloudWatch Log API access
    logs_client: Any
    log_group_name: str
    log_stream_name: str
    start_time: Optional[datetime]
    end_time: Optional[datetime]

    def __init__(self, logs_client, log_group_name, log_stream_name, start_time, end_time):
        self.events = []
        self.position = -1
        self.nav_tokens = []
        self.contains_head = False
        self.contains_tail = False

        self.logs_client = logs_client
        self.log_group_name = log_group_name
        self.log_stream_name = log_stream_name
        self.start_time = start_time
        self.end_time = end_time

    def clear_events(self):
        self.events = []
        self.position = -1
        self.nav_tokens = []
        self.contains_head = False
        self.contains_tail = False

    def position_index(self, display_row_count):
        if self.position == -1 or self.position + display_row_count >= len(self.events):
            return len(self.events) - display_row_count
        else:
            return self.position

    def apply_position_offset(self, position_offset, display_row_count):
        if position_offset == 0:
            return

        position = self.position_index(display_row_count)

        position += position_offset
        if position >= len(self.events) - display_row_count:
            position = -1
        elif position < 0:
            position = 0
        self.position = position

    def prepend_events(self, events, next_backward_token, next_forward_token, contains_head):
        if len(events) == 0:
            return

        if len(self.events) == 0:
            self.events = events
            self.position = -1
            self.nav_tokens = [
                SlidingLogPortholeQuery(next_backward_token, next_forward_token, 0, len(events))
            ]
            self.contains_head = contains_head
            self.contains_tail = False
        else:
            nav_tokens = [
                SlidingLogPortholeQuery(next_backward_token, next_forward_token, 0, len(events))
            ]
            nav_tokens.extend(
                SlidingLogPortholeQuery(
                    q.next_backward_token,
                    q.next_forward_token,
                    q.events_start_index + len(events),
                    q.events_end_index + len(events),
                )
                for q in self.nav_tokens
            )

            self.events = events + self.events
            if self.position != -1:
                self.position += len(events)
            self.nav_tokens = nav_tokens
            self.contains_head = contains_head

    def append_events(self, events, next_backward_token, next_forward_token, contains_tail):
        if len(events) == 0:
            return

        if len(self.events) == 0:
            self.events = events
            self.position = -1
            self.nav_tokens = [
                SlidingLogPortholeQuery(next_backward_token, next_forward_token, 0, len(events))
            ]
            self.contains_head = False
            self.contains_tail = contains_tail
        else:
            events_start_index = len(events)
            self.events.extend(events)
            events_end_index = len(events)
            self.nav_tokens.append(
                SlidingLogPortholeQuery(
                    next_backward_token, next_forward_token, events_start_index, events_end_index
                )
            )
            self.contains_tail = contains_tail

    def grow_forward(self, *, count: int, reset_to_tail: bool = False):
        if reset_to_tail and self.contains_tail and self.end_time is not None:
            self.position = -1
            return

        if reset_to_tail or len(self.nav_tokens) == 0:
            tail_events, next_backward_token, next_forward_token = get_log_events_backward(
                self.logs_client,
                self.log_group_name,
                self.log_stream_name,
                count=count,
                start_time=self.start_time,
                end_time=self.end_time,
            )
            # Reset the self
            self.clear_events()
            self.append_events(
                tail_events, next_backward_token, next_forward_token, contains_tail=True
            )
            self.position = -1
            self.contains_head = len(tail_events) < count
        else:
            tail_events, next_backward_token, next_forward_token = get_log_events_forward(
                self.logs_client,
                self.log_group_name,
                self.log_stream_name,
                count=count,
                start_time=self.start_time,
                end_time=self.end_time,
                next_forward_token=self.nav_tokens[-1].next_forward_token,
            )
            # If the API returned the same next_forward_token, the result contains head
            contains_tail = next_forward_token == self.nav_tokens[0].next_forward_token
            if self.contains_tail and self.position == -1 and not contains_tail:
                # In this case, we want to stick to the tail, so reset to the tail again
                self.grow_forward(count=count, reset_to_tail=True)
                return
            self.append_events(
                tail_events, next_backward_token, next_forward_token, contains_tail=contains_tail
            )

    def grow_backward(self, *, count: int, reset_to_head: bool = False):
        if reset_to_head and self.contains_head:
            self.position = 0
            return

        if reset_to_head or len(self.nav_tokens) == 0:
            head_events, next_backward_token, next_forward_token = get_log_events_forward(
                self.logs_client,
                self.log_group_name,
                self.log_stream_name,
                count=count,
                start_time=self.start_time,
                end_time=self.end_time,
            )
            # Replace the self
            self.clear_events()
            self.prepend_events(
                head_events, next_backward_token, next_forward_token, contains_head=True
            )
            self.position = 0
            self.contains_tail = len(head_events) < count
        else:
            events, next_backward_token, next_forward_token = get_log_events_backward(
                self.logs_client,
                self.log_group_name,
                self.log_stream_name,
                count=count,
                start_time=self.start_time,
                end_time=self.end_time,
                next_backward_token=self.nav_tokens[0].next_backward_token,
            )
            # If the API returned the same next_backward_token, the result contains head
            contains_head = next_backward_token == self.nav_tokens[0].next_backward_token
            self.prepend_events(
                events, next_backward_token, next_forward_token, contains_head=contains_head
            )

    def move_to_timestamp(self, *, timestamp: datetime, count: int, display_row_count: int):
        start_time = self.start_time
        end_time = self.end_time
        if end_time is None:
            end_time = datetime.now(tz=timezone.utc)

        if len(self.events) > 0:
            events_start_time = self.events[0][0]
            events_end_time = self.events[-1][0]
        else:
            # An empty interval in this case
            events_start_time = start_time
            events_end_time = start_time - timedelta(seconds=1)

        if end_time - timestamp > timestamp - start_time:
            # If we're in the first half of the scroll range, use the insertion point as the start of the porthole
            if len(self.events) > 0 and events_start_time <= timestamp <= events_end_time:
                # The timestamp is within the porthole range
                position = bisect_left(self.events, (timestamp,))
                if position + display_row_count > len(self.events):
                    self.grow_forward(count=count)
                position = max(
                    0,
                    min(
                        bisect_left(self.events, (timestamp,)), len(self.events) - display_row_count
                    ),
                )
                self.position = position
            else:
                # The timestamp is outside the porthole range
                events, next_backward_token, next_forward_token = get_log_events_forward(
                    self.logs_client,
                    self.log_group_name,
                    self.log_stream_name,
                    count=count,
                    start_time=timestamp,
                    end_time=self.end_time,
                )
                if len(events) == 0:
                    self.grow_forward(count=count, reset_to_tail=True)
                else:
                    self.clear_events()
                    self.prepend_events(events, next_backward_token, next_forward_token, contains_head=False)
                    self.position = 0
        else:
            # If we're in the second half of the scroll range, use the insertion point as the end of the porthole
            if len(self.events) > 0 and events_start_time <= timestamp <= events_end_time:
                # The timestamp is within the porthole range
                position = bisect_right(self.events, (timestamp,)) - display_row_count
                if position < 0:
                    self.grow_backward(count=count)
                position = max(0, bisect_right(self.events, (timestamp,)) - display_row_count)
                self.position = position
            else:
                # The timestamp is outside the porthole range
                events, next_backward_token, next_forward_token = get_log_events_backward(
                    self.logs_client,
                    self.log_group_name,
                    self.log_stream_name,
                    count=count,
                    start_time=self.start_time,
                    end_time=timestamp,
                )
                if len(events) == 0:
                    self.grow_backward(count, reset_to_head=True)
                else:
                    self.clear_events()
                    self.append_events(
                        events, next_backward_token, next_forward_token, contains_tail=False
                    )
                    self.position = -1


class CloudWatchLogViewWidget(QWidget):
    """
    This widget uses virtual scrolling to display a CloudWatch Logs Logstream, with optional
    timestamp bounds.
    """

    def __init__(
        self,
        *,
        boto3_deadline_client,
        boto3_logs_client,
        start_time=None,
        end_time=None,
        row_height=18,
        parent: QWidget = None,
    ) -> None:
        super().__init__(parent=parent)

        self.deadline_client = boto3_deadline_client
        self.logs_client = boto3_logs_client

        self.log_group_name = "/aws/deadline/farm-9e198d0d5a7b48e5863119fe20960597/queue-5770e86cfcd446db87f2776ec188e6f7"
        self.log_stream_name = "session-9b4e19f90c004c2b99394ad4f6980b75" # "session-2feefadefc03467495964835550a7382"

        self.start_time = start_time
        self.end_time = end_time

        # This is the tail of the log
        self.porthole = SlidingLogPorthole(
            self.logs_client,
            self.log_group_name,
            self.log_stream_name,
            self.start_time,
            self.end_time,
        )
        self.saved_scrollbar_value = 0
        self.saved_scrollbar_is_index = False

        self.row_height = row_height

        self._build_ui()
        self.porthole.grow_forward(count=max(250, 2 * self.table.rowCount()), reset_to_tail=True)
        self._fill_events()

    def _build_ui(self) -> None:
        layout = QHBoxLayout(self)

        self.table = QTableWidget(self)
        layout.addWidget(self.table)

        self.table.setColumnCount(2)
        self.table.setRowCount(1)
        self.table.setHorizontalHeaderLabels(["Timestamp", "Log Entry"])
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.verticalHeader().setSectionResizeMode(QHeaderView.Fixed)
        self.table.verticalHeader().hide()

        self.table.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        self.scrollbar = QScrollBar(self)
        layout.addWidget(self.scrollbar)

        self.scrollbar.setRange(0, 0)
        self.scrollbar.setPageStep(10000)
        self.scrollbar.actionTriggered.connect(self.scrollbarActionTriggered)

        self._update_row_count()

    def _update_row_count(self):
        # Update the row count to the biggest count that fits
        header_height = self.table.horizontalHeader().height()
        real_row_height = self.table.rowHeight(0)
        row_count = max((self.scrollbar.height() - header_height) // real_row_height, 1)
        self.table.setRowCount(row_count)
        for i in range(row_count):
            self.table.setRowHeight(i, self.row_height)

    def resizeEvent(self, event):
        self._update_row_count()
        self._fill_events()

    def scrollbarActionTriggered(self, event):
        porthole = self.porthole
        display_row_count = self.table.rowCount()

        scrollbar_timestamp = None
        if event == QAbstractSlider.SliderSingleStepAdd:
            position_offset = 1
        elif event == QAbstractSlider.SliderSingleStepSub:
            position_offset = -1
        elif event == QAbstractSlider.SliderPageStepAdd:
            position_offset = display_row_count
        elif event == QAbstractSlider.SliderPageStepSub:
            position_offset = -display_row_count
        elif event == QAbstractSlider.SliderToMinimum:
            self.porthole.grow_backward(
                count=max(250, 2 * self.table.rowCount()), reset_to_head=True
            )
        elif event == QAbstractSlider.SliderToMaximum:
            self.porthole.grow_forward(
                count=max(250, 2 * self.table.rowCount()), reset_to_tail=True
            )
        elif event == QAbstractSlider.SliderMove:
            if self.saved_scrollbar_value == self.scrollbar.value():
                return

            if self.saved_scrollbar_is_index:
                position_offset = self.scrollbar.value() - self.saved_scrollbar_value
            else:
                position_offset = 0
                scrollbar_timestamp = self.start_time + timedelta(milliseconds=self.scrollbar.value())
                self.porthole.move_to_timestamp(
                    timestamp=scrollbar_timestamp,
                    count=max(250, 2 * self.table.rowCount()),
                    display_row_count=self.table.rowCount(),
                )
        else:
            print(f"Unhandled scrollbar action event {event}")
            return

        if position_offset != 0:
            position = porthole.position_index(display_row_count)

            # If we would spill over the edge, get more items
            if position + position_offset + display_row_count > len(porthole.events):
                self.porthole.grow_forward(count=max(250, 2 * self.table.rowCount()))
            elif position + position_offset < 0:
                self.porthole.grow_backward(count=max(250, 2 * self.table.rowCount()))

            porthole.apply_position_offset(position_offset, display_row_count)

        self._fill_events(scrollbar_timestamp=scrollbar_timestamp)
        self.saved_scrollbar_value = self.scrollbar.value()

    def _fill_events(self, *, scrollbar_timestamp: datetime=None):
        """
        Args:
            scrollbar_timestamp (datetime) - If provided, use this directly as the scrollbar timestamp,
                instead of using the log entry timestamp. This preserves scrollbar behavior.
        """
        porthole = self.porthole

        # If we got events, but there's no start time yet, determine the start time
        if len(porthole.events) != 0 and self.start_time is None:
            if porthole.contains_head:
                # This is the first event
                self.start_time = porthole.events[0][0]
            else:
                # Query for the first event
                head_events, _, _ = get_log_events_forward(
                    self.logs_client, self.log_group_name, self.log_stream_name, count=1
                )
                self.start_time = head_events[0][0]
            porthole.start_time = self.start_time

        with block_signals(self.table):
            display_row_count = self.table.rowCount()
            event_count = len(porthole.events)
            if event_count < display_row_count:
                self.table.setRowCount(event_count)
                display_row_count = event_count

            if porthole.position == -1 or porthole.position + display_row_count >= event_count:
                porthole.position = -1
                start_index = event_count - display_row_count
            else:
                start_index = porthole.position

            for i in range(display_row_count):
                event = porthole.events[start_index + i]
                self.table.setItem(i, 0, QTableWidgetItem(str(event[0] - self.start_time)[:-3]))
                self.table.setItem(i, 1, QTableWidgetItem(event[1]))

        # Update the scrollbar based on what we filled in
        with block_signals(self.scrollbar):
            if self.porthole.contains_head and self.porthole.contains_tail:
                # We have all the events, so use its list for the scrollbar
                self.scrollbar.setPageStep(display_row_count)
                if event_count == display_row_count:
                    self.scrollbar.setRange(0, 0)
                    self.scrollbar.setSliderPosition(0)
                else:
                    self.scrollbar.setRange(0, event_count - display_row_count)
                    self.scrollbar.setSliderPosition(start_index)

                # Indicate that the scroll values are event indexes
                self.saved_scrollbar_is_index = True
            else:
                # We don't have all the events, so use timestamps for the scrollbar
                end_time = self.end_time
                if end_time is None:
                    end_time = datetime.now(tz=timezone.utc)
                end_position = int((end_time - self.start_time).total_seconds() * 1000)
                page_step = int(
                    (
                        porthole.events[start_index + display_row_count - 1][0]
                        - porthole.events[start_index][0]
                    ).total_seconds()
                    * 1000
                )
                if scrollbar_timestamp is None:
                    slider_position = int(
                        (porthole.events[start_index][0] - self.start_time).total_seconds() * 1000
                    )
                else:
                    slider_position = int(
                        (scrollbar_timestamp - self.start_time).total_seconds() * 1000
                    )

                self.scrollbar.setPageStep(page_step)
                self.scrollbar.setRange(0, end_position - page_step)
                self.scrollbar.setSliderPosition(slider_position)

                # Indicate that the scroll values are timestamps
                self.saved_scrollbar_is_index = False
