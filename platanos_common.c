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

#include"../api/platanos.h"
#include"../api/platanos_common.h"
#include"platanos_structs.h"
#include<string.h>


typedef struct platanos_node_t platanos_node_t;



void platanos_online_bind_points (zhandle_t * zh, char *octopus, char *comp_name,
                           char *res_name, char *bind_points[][9], int *size){

*bind_points = malloc(50*1);

 int buffer_len;

buffer_len = 1000;
            sprintf (path, "/%s/computers/%s/worker_nodes/%s/bind_point",
                     octopus, comp_name, res_name);
            result =
                zoo_get (ozookeeper->zh, path, 0, *bind_points[0], &buffer_len,
                         &stat);
            assert(buffer_len<51);
            assert (result == ZOK);

*size=1;


}

void platanos_node_init( platanos_node_t **plananos_node){

*platanos_node = malloc(sizeof(platanos_node_t));

}

void platanos_node_destroy (platanos_node_t ** platanos_node){
free(*platanos);
*platanos=NULL;
}

platanos_node_t *platanos_node_dup (platanos_node_t * platanos_node){

platanos_node_t *node_dup= malloc(sizeof(struct platanos_node_t));

strcpy(node_dup->bind_point,platanos_node->bind_point);
return node_dup;
}

