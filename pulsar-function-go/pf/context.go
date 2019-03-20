/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package pf

import (
	"context"
)

type FunctionContext struct {
	InstanceConf *InstanceConf
	UserConfigs  map[string]interface{}
	InputTopics  []string
}

func NewFuncContext() *FunctionContext {
	fc := &FunctionContext{
		InstanceConf: NewInstanceConf(),
		UserConfigs:  make(map[string]interface{}),
	}
	return fc
}

func (c *FunctionContext) GetInstanceID() int {
	return c.InstanceConf.InstanceID
}

func (c *FunctionContext) GetInputTopics() []string {
	return c.InputTopics
}

func (c *FunctionContext) GetOutputTopic() string {
	return c.InstanceConf.FuncDetails.GetSink().Topic
}

func (c *FunctionContext) GetFuncTenant() string {
	return c.InstanceConf.FuncDetails.Tenant
}

func (c *FunctionContext) GetFuncName() string {
	return c.InstanceConf.FuncDetails.Name
}

func (c *FunctionContext) GetFuncNamespace() string {
	return c.InstanceConf.FuncDetails.Namespace
}

func (c *FunctionContext) GetFuncID() string {
	return c.InstanceConf.FuncID
}

func (c *FunctionContext) GetFuncVersion() string {
	return c.InstanceConf.FuncVersion
}

func (c *FunctionContext) GetUserConfValue(key string) interface{} {
	return c.UserConfigs[key]
}

func (c *FunctionContext) GetUserConfMap() map[string]interface{} {
	return c.UserConfigs
}

// An unexported type to be used as the key for types in this package.
// This prevents collisions with keys defined in other packages.
type key struct{}

// contextKey is the key for user.User values in Contexts. It is
// unexported; clients use user.NewContext and user.FromContext
// instead of using this key directly.
var contextKey = &key{}

// NewContext returns a new Context that carries value u.
func NewContext(parent context.Context, fc *FunctionContext) context.Context {
	return context.WithValue(parent, contextKey, fc)
}

// FromContext returns the User value stored in ctx, if any.
func FromContext(ctx context.Context) (*FunctionContext, bool) {
	fc, ok := ctx.Value(contextKey).(*FunctionContext)
	return fc, ok
}
