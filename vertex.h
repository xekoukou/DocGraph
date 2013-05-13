/*
    Copyright contributors as noted in the AUTHORS file.
                
    This file is part of PLATANOS.

    PLATANOS is free software; you can redistribute it and/or modify it under
    the terms of the GNU Affero General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.
            
    PLATANOS is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.
        
    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include"../api/vertex.h"

#ifndef _OCTOPUS_GPROJECT_VERTEX_H_
#define _OCTOPUS_GPROJECT_VERTEX_H_

struct edge_t {
unsigned char position;
int64_t key;
};

typedef struct edge_t edge_t;

struct skip_vertex_t {
int64_t *for_edges[];
int64_t for_edges_size;

int64_t *bck_edges[];
int64_t bck_edges_size;


};

typedef struct skip_vertex_t skip_vertex_t;

struct vertex_t {
int64_t key;
edge_t *edges[];      //ordered by position
int64_t edges_size;

uint64_t *bck_edges[];
int64_t bck_edges_size;

skip_vertex_t *skip_vertex[];
int64_t skip_size;
};


#endif
