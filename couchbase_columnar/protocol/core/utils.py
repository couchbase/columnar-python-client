#  Copyright 2016-2024. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


from datetime import timedelta
from time import time

THIRTY_DAYS_IN_SECONDS = 30 * 24 * 60 * 60


def timedelta_as_timestamp(duration: timedelta) -> int:
    if not isinstance(duration, timedelta):
        raise ValueError(f'Expected timedelta instead of {duration}')

    # PYCBC-1177 remove deprecated heuristic from PYCBC-948:
    seconds = int(duration.total_seconds())
    if seconds < 0:
        raise ValueError(f'Expected expiry seconds of zero (for no expiry) or greater, got {seconds}.')

    if seconds < THIRTY_DAYS_IN_SECONDS:
        return seconds

    return seconds + int(time())
