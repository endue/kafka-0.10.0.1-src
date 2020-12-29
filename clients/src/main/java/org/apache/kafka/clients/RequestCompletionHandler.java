/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients;

/**
 * A callback interface for attaching an action to be executed when a request is complete and the corresponding response
 * has been received. This handler will also be invoked if there is a disconnection while handling the request.
 * 一个回调接口，用于附加一个动作，以便在请求完成并接收到相应的响应时执行.
 * 如果在处理请求时出现断开连接，也将调用此处理程序
 */
public interface RequestCompletionHandler {

    public void onComplete(ClientResponse response);

}
