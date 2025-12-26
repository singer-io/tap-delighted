from tap_delighted.streams.bounces import Bounces
from tap_delighted.streams.email_autopilot import EmailAutopilot
from tap_delighted.streams.metrics import Metrics
from tap_delighted.streams.people import People
from tap_delighted.streams.sms_autopilot import SmsAutopilot
from tap_delighted.streams.survey_responses import SurveyResponses
from tap_delighted.streams.unsubscribes import Unsubscribes

STREAMS = {
    "people": People,
    "survey_responses": SurveyResponses,
    "metrics": Metrics,
    "unsubscribes": Unsubscribes,
    "bounces": Bounces,
    "email_autopilot": EmailAutopilot,
    "sms_autopilot": SmsAutopilot,
}
