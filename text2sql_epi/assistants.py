import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import requests
import tiktoken
from openai import AsyncAzureOpenAI

from text2sql_epi.settings import settings

logger = logging.getLogger(__name__)


class GPTAssistant:
    def __init__(self, engine=None):
        self.engine = engine
        self.system_message = {
            "role": "system",
            "content": "You are a helpful assistant.",
        }
        self.max_response_tokens = 4096
        self.token_limit = 8192 * 2
        self.conversation = [self.system_message]
        self.client = AsyncAzureOpenAI(
            api_key=settings.OPENAI_API_KEY,
            api_version=settings.OPENAI_API_VERSION,
            azure_endpoint=settings.OPENAI_API_BASE,
        )

    def num_tokens_from_messages(self, messages):
        encoding = tiktoken.encoding_for_model("gpt-4-32k")
        num_tokens = 0
        for message in messages:
            num_tokens += (
                4  # every message follows <im_start>{role/name}\n{content}<im_end>\n
            )
            for key, value in message.items():
                num_tokens += len(encoding.encode(value))
                if key == "name":  # if there's a name, the role is omitted
                    num_tokens += -1  # role is always required and always 1 token
        num_tokens += 2  # every reply is primed with <im_start>assistant
        return num_tokens

    def add_message(self, role, message):
        self.conversation.append({"role": role, "content": message})
        self.manage_conversation_length()

    def reset_conversation(self):
        self.conversation = [self.system_message]

    def manage_conversation_length(self):
        conv_history_tokens = self.num_tokens_from_messages(self.conversation)

        while conv_history_tokens + self.max_response_tokens >= self.token_limit:
            del self.conversation[1]
            conv_history_tokens = self.num_tokens_from_messages(self.conversation)

    async def get_response(self, prompt: Optional[str] = None):
        messages = (
            [{"role": "user", "content": prompt}]
            if prompt is not None
            else self.conversation
        )
        try:
            response = await self.client.chat.completions.create(
                model=self.engine,
                temperature=0,
                messages=messages,
                max_tokens=self.max_response_tokens,
            )
        except Exception as err:
            logger.exception("An error occurred.")
            raise err
        logger.info(
            f"Successful GPT response! endpoint: {settings.OPENAI_API_BASE}, model: {self.engine}, usage: {str(response.usage)}, utc-timestamp: {datetime.now(timezone.utc).strftime('%Y.%m.%d %H:%M')}, message:{str(messages)}, response-content: {response.choices[0].message.content}"
        )
        if prompt is None:
            self.add_message(
                role="assistant", message=response.choices[0].message.content
            )

        return response.choices[0].message.content

    async def get_response_json(self, prompt: Optional[str] = None):
        messages = (
            [{"role": "user", "content": prompt}]
            if prompt is not None
            else self.conversation
        )
        logger.info(
            f"Sending GPT request... endpoint: {settings.OPENAI_API_BASE}, model: {self.engine}, message-tokens: {self.num_tokens_from_messages(messages)}, max_response_tokens: {self.max_response_tokens}, utc-timestamp: {datetime.now(timezone.utc).strftime('%Y.%m.%d %H:%M')}, message:{str(messages)}"
        )
        try:
            response = await self.client.chat.completions.create(
                model=self.engine,
                response_format={"type": "json_object"},
                temperature=0,
                messages=messages,
                max_tokens=self.max_response_tokens,
            )
        except Exception as err:
            logger.exception("An error occurred")
            raise err
        logger.info(
            f"Successful GPT response! endpoint: {settings.OPENAI_API_BASE}, model: {self.engine}, message-tokens: {self.num_tokens_from_messages(messages)}, max_response_tokens: {self.max_response_tokens}, utc-timestamp: {datetime.now(timezone.utc).strftime('%Y.%m.%d %H:%M')}, message:{str(messages)}, response-content: {response.choices[0].message.content}"
        )
        if prompt is None:
            self.add_message(
                role="assistant", message=response.choices[0].message.content
            )

        return response.choices[0].message.content


class MistralAssistant:
    def __init__(self, model, mistral_api_key=None):

        if mistral_api_key is None:
            self.mistral_api_key = os.getenv("MISTRAL_API_KEY")
        else:
            self.mistral_api_key = mistral_api_key
        self.url = "https://api.mistral.ai/v1/chat/completions"
        self.model = model

    def get_response(self, message):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.mistral_api_key}",
        }

        data = {"model": self.model, "messages": [{"role": "user", "content": message}]}

        response = requests.post(self.url, headers=headers, data=json.dumps(data))

        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"]
        else:
            return response.text

    def reset_conversation(self):
        pass


def get_engine_from_assistant_type(assistant_type):
    if assistant_type == "gpt4turbo":
        assistant_engine = "gpt-4-turbo-1106-preview-ascent"
    elif assistant_type == "gpt4turbo-south":
        assistant_engine = "gpt-4-turbo-0125-preview-ascent"
    elif assistant_type == "gpt4":
        assistant_engine = "gpt_4_32k_ascent_0613"
    elif assistant_type == "gpt35":
        assistant_engine = "gpt-35-turbo-1106-ascent"
    else:
        assistant_engine = None
    return assistant_engine


def create_assistant(assistant_type):
    assistant_classes = {
        "gpt4turbo": GPTAssistant,
        "gpt4": GPTAssistant,
        "gpt35": GPTAssistant,
        "mistral-tiny": MistralAssistant,
        "mistral-small": MistralAssistant,
        "mistral-medium": MistralAssistant,
    }

    engine = get_engine_from_assistant_type(assistant_type)

    if isinstance(assistant_type, str) and assistant_type in assistant_classes:
        if (
            assistant_type == "gpt4"
            or assistant_type == "gpt4turbo"
            or assistant_type == "gpt4turbo-south"
            or assistant_type == "gpt35"
        ):
            assistant = assistant_classes[assistant_type](engine=engine)
        elif (
            assistant_type == "mistral-tiny"
            or assistant_type == "mistral-small"
            or assistant_type == "mistral-medium"
        ):
            assistant = assistant_classes[assistant_type](model=assistant_type)
        else:
            assistant = assistant_classes[assistant_type]()
        return assistant
    else:
        logger.exception("Please specify a valid assistant")
        return None
