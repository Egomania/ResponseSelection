#  This file is part of CSR Optimizer.
# (C) 2013 Matthias Wachs (and other contributing authors)
#
# GNUnet is free software; you can redistribute it and/or modify3
# it under the terms of the GNU General Public License as published
# by the Free Software Foundation; either version 3, or (at your
# option) any later version.
#
# GNUnet is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.3
#
# You should have received a copy of the GNU General Public License
# along with GNUnet; see the file COPYING.  If not, write to the
# Free Software Foundation, Inc., 59 Temple Place - Suite 330,
# Boston, MA 02111-1307, USA.
#

#
# Author: Matthias Wachs (<lastname> [at] net.in.tum.de )
#
class Response(object):
    '''
    classdocs
    '''
    name = ""
    source = None
    dest = None
    metrics = None
    conflicting_responses = None

    def __init__(self, name="undefined", src=None, dest=None, metrics=None, conflicts=None):
        '''
        Constructor
        '''
        self.name = name
        self.source = src
        self.dest = dest
        self.metrics = metrics
        self.conflicting_responses = conflicts
    def get_cost (self, metric):
        for m in self.metrics:
            if (m.name == metric):
                return m.value
        return None