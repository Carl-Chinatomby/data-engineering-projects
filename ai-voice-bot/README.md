This initially starts from the tutorial in:

https://www.assemblyai.com/blog/real-time-ai-voice-bot-python/

and then evolved with modifications.


Instructions:
Install pyenv and  poetry. This is built using python 3.12.

`pyenv virtualenv ai-voice-bot`
`pyenv activate ai-voice-bot`
`poetry install`
`cp .env_template .env`
Fill out the environment variables.
`(export $(cat .env | xargs) && python ai_voice_bot/main.py)`
