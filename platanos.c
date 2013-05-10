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


#include"api/platanos.h"


struct platanos_poll_t{

zmq_pollitem_t pollitem;
int64_t next_time;
};

struct platanos_t{
platanos_poll_t *poll;
void *router;
void *dealer;
compute_t *compute;

};


void platanos_poll_init (platanos_poll_t ** poll,platanos_t *platanos){

*poll=malloc(sizeof(struct platanos_poll_t));

(*poll)->pollitem = (zmq_pollitem_t) {
    platanos->dealer, 0, ZMQ_POLLIN};

(*poll)->next_time=-1;

}

int64_t platanos_poll_before_poll (platanos_poll_t * poll){

return poll->next_time;
}

void platanos_poll_pollitems (platanos_poll_t * poll,
                              zmq_pollitem_t ** pollitems, int *size){

*pollitems=&(poll->pollitem);
*size=1;
}

void platanos_do (platanos_t * platanos);

platanos_poll_t *platanos_poll (platanos_t * platanos){
return platanos->poll;
}

void platanos_init (platanos_t ** platanos,
                    compute_t * compute, char *id, zctx_t * ctx){

*platanos=malloc(sizeof(platanos_t));

    (*platanos)->router = zsocket_new (ctx, ZMQ_ROUTER);
    (*platanos)->dealer = zsocket_new (ctx, ZMQ_DEALER);
    zmq_setsockopt ((*platanos)->dealer, ZMQ_IDENTITY, id, strlen (id));

platanos_poll_init(&((*platanos)->poll),*platanos);
(*platanos)->compute=compute;
}


void platanos_register (zhandle_t * zh, char *octopus,char *comp_name, char *res_name,
                        char *bind_point,oconfig_t *config){

          char bind_location[1000];
          int  port = oconfig_incr_port (fconfig);
            sprintf (bind_location, "tcp://%s:%d", bind_point, port);
            sprintf (path, "/%s/computers/%s/worker_nodes/%s/bind_point", octopus,
                     comp_name, res_name);
          int  result =
                zoo_create (zh, path, bind_location,
                            strlen (bind_location) + 1, &ZOO_OPEN_ACL_UNSAFE,
                            0, NULL, 0);


            assert (ZOK == result);



}


void platanos_send (platanos_t * platanos, zmsg_t * msg);

platanos_node_t *platanos_connect (platanos_t * platanos, zmsg_t * msg){
    char bind_point[50];
    frame = zmsg_next (msg);
    memcpy (bind_point, zframe_data (frame), zframe_size (frame));

zmsg_destroy(&msg);

   int rc;
    rc = zsocket_connect (platanos->router, "%s", bind_point);
    assert (rc == 0);

platanos_node_t *node = malloc(sizeof(platanos_node_t));

strcpy(node->bind_point,bind_point);
return node;
}


platanos_node_t *platanos_bind (platanos_t * platanos, zmsg_t * msg){
    char bind_point[50];
    frame = zmsg_next (msg);
    memcpy (bind_point, zframe_data (frame), zframe_size (frame));

zmsg_destroy(&msg);

   int rc;
    rc = zsocket_bind (platanos->dealer, "%s", bind_point);
    assert (rc != -1);

platanos_node_t *node = malloc(sizeof(platanos_node_t));

strcpy(node->bind_point,bind_point);
return node;

}

