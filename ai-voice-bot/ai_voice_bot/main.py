import os

import assemblyai as aai
from elevenlabs import stream
from elevenlabs.client import ElevenLabs
from openai import OpenAI


class AI_Assistant:
    def __init__(self):
        aai.settings.api_key = os.environ.get("AAI_API_KEY")
        self.open_ai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        self.elevenlabs_api_key = os.environ.get("ELEVENLABS_API_KEY")

        self.transcriber = None

        # Prompt
        self.full_transcript = [
            {
                "role": "system",
                "content": "Welcome to CarlBot. Ask me questions about Carl."
            },
        ]
