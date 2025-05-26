import os

import assemblyai as aai
from elevenlabs import stream
from elevenlabs.client import ElevenLabs
from openai import OpenAI


class AI_Assistant:
    def __init__(self, sample_rate=16000):
        aai.settings.api_key = os.environ.get("AAI_API_KEY")
        self.open_ai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        self.el_client = ElevenLabs(api_key = os.environ.get("ELEVENLABS_API_KEY"))
        self.sample_rate = sample_rate
        self.transcriber = None

        # Prompt
        self.full_transcript = [
            {
                "role": "system",
                "content": "You are a receptionist at a bowling alley. Be resourceful and efficient."
            },
        ]

    def stop_transcription(self):
        if self.transcriber:
            self.transcriber.close()
            self.transcriber = None

    def on_open(self, session_opened: aai.RealtimeSessionOpened):
        print("Session ID:", session_opened.session_id)
        return

    def on_error(self, error: aai.RealtimeError):
        print("An error occured:", error)
        return

    def on_close(self):
        print("Closing  Session")
        return

    def on_data(self, transcript: aai.RealtimeTranscript):
        if not transcript.text:
            return

        if isinstance(transcript, aai.RealtimeFinalTranscript):
            self.generate_ai_response(transcript)
        else:
            print(transcript.text, end='\r')

    def start_transcription(self):
        self.transcriber = aai.RealtimeTranscriber(
            sample_rate = self.sample_rate,
            on_data = self.on_data,
            on_error = self.on_error,
            on_open = self.on_open,
            on_close = self.on_close,
            end_utterance_silence_threshold = 1000,
        )

        self.transcriber.connect()
        microphone_stream = aai.extras.MicrophoneStream(sample_rate = self.sample_rate)
        self.transcriber.stream(microphone_stream)

    def generate_ai_response(self, transcript):
        self.stop_transcription()

        self.full_transcript.append({"role": "user", "content": transcript.text})
        print(f"\nUser: {transcript.text}", end="\r\n")

        response = self.open_ai_client.chat.completions.create(
            model = "gpt-3.5-turbo",
            messages = self.full_transcript
        )

        ai_response = response.choices[0].message.content

        self.generate_audio(ai_response)

        self.start_transcription()
        print(f"\nReal-time transcription: ", end="\r\n")

    def generate_audio(self, text):
        self.full_transcript.append({"role": "assistant", "content": text})
        print(f"\nAI Carl Responder: {text}")

        audio_stream = self.el_client.generate(
            text=text,
            voice="Rachel",
            model="eleven_multilingual_v2",
            stream=True,
        )
        stream(audio_stream)


if __name__ == "__main__":
    greeting = "Thank you for activating Carl Bowling Bot. Ask me questions before talking to Carl. What would you like to know?"
    ai_assistant = AI_Assistant()
    ai_assistant.generate_audio(greeting)
    ai_assistant.start_transcription()
