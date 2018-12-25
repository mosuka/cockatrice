# -*- coding: utf-8 -*-

# Copyright (c) 2018 Minoru Osuka
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 		http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from janome.tokenizer import Tokenizer as Janome
from whoosh.analysis import Token, Tokenizer
from whoosh.compat import text_type


class JanomeTokenizer(Tokenizer):
    def __init__(self, udic='', udic_enc='utf8', udic_type='ipadic', max_unknown_length=1024, wakati=False, mmap=False):
        self.udic = udic
        self.udic_enc = udic_enc
        self.udic_type = udic_type
        self.max_unknown_length = max_unknown_length
        self.wakati = wakati
        self.mmap = mmap

        self.tagger = Janome(udic=self.udic, udic_enc=self.udic_enc, udic_type=self.udic_type,
                             max_unknown_length=self.max_unknown_length, wakati=self.wakati, mmap=self.mmap)

    def __getstate__(self):
        return {k: v for k, v in self.__dict__.items() if k != "tagger"}

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.tagger = Janome(udic=self.udic, udic_enc=self.udic_enc, udic_type=self.udic_type,
                             max_unknown_length=self.max_unknown_length, wakati=self.wakati, mmap=self.mmap)
        self.tagger.tokenize('')

    def __call__(self, value, positions=False, chars=False, keeporiginal=False, removestops=True, start_pos=0,
                 start_char=0, tokenize=True, mode='', **kwargs):
        assert isinstance(value, text_type), '%s is not unicode' % repr(value)

        token = Token(positions, chars, removestops=removestops, mode=mode, **kwargs)

        if not tokenize:
            token.original = token.text = value
            token.boost = 1.0
            if positions:
                token.pos = start_pos
            if chars:
                token.startchar = start_char
                token.endchar = start_char + len(value)
            yield token
        else:
            pos = start_pos
            for janome_token in self.tagger.tokenize(value):
                token.text = janome_token.surface
                token.boost = 1.0
                if keeporiginal:
                    token.original = token.text
                token.stopped = False
                if positions:
                    token.pos = pos
                    pos += 1
                if chars:
                    token.startchar = start_char + janome_token.start
                    token.endchar = token.startchar + len(janome_token.surface)
                yield token
