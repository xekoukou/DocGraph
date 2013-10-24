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
#include"platanos_structs.h"


void
platanos_send (platanos_t * platanos, zmsg_t * msg)
{
    int64_t key;

    memcpy (&key, zframe_data (zmsg_first (msg)), sizeof (int64_t));

    router_route (platanos->compute->router, key);


    zframe_t *address = zframe_new (key, strlen (key));

    zmsg_wrap (msg, address);

    zmsg_send (msg, platanos->router);

}


void
platanos_do (platanos_t * platanos)
{

    if (platanos->poll->pollitem.revents & ZMQ_POLLIN) {

        zmsg_t *msg = zmsg_recv (platanos->dealer);

        int64_t key;
        memcpy (&key, zframe_data (zmsg_first (msg)), sizeof (int64_t));

        khiter_t k;
        k = kh_get (vertices, platanos->compute->hash, key);

//missing
        if (k == kh_end (platanos->compute->hash)) {
            if (intervals_belongs (platanos->compute->intervals, key)) {
//TODO load vertex
            }
            else {
                platanos_send (platanos, msg);
            }
        }
        else {
            zframe_t *frame = zmsg_pop (msg);
            zframe_destroy (&frame);

            vertex_t *vertex = &kh_value (platanos->compute->hash, k);

            zframe_t *frame = zmsg_pop (msg);
            unsigned char function;
            memcpy (&function, zframe_data (frame), 1);
            zframe_destroy (&frame);

            switch (function) {

            case 0:
                break;
            default:
                assert (1 == 0);
            }

        }

    }
}
