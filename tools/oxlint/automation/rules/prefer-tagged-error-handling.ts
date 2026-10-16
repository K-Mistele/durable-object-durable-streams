import { defineRule } from '@oxlint/plugins'
import type { ESTree } from '@oxlint/plugins'

const broadCatchMethods = new Set(['catch', 'catchIf'])

const isEffectBroadCatch = (node: ESTree.CallExpression): boolean =>
	node.callee.type === 'MemberExpression' &&
	node.callee.object.type === 'Identifier' &&
	node.callee.object.name === 'Effect' &&
	node.callee.property.type === 'Identifier' &&
	broadCatchMethods.has(node.callee.property.name)

export default defineRule({
	meta: {
		type: 'problem',
		docs: {
			description: 'Use Effect tagged error handlers instead of Predicate.isTagged in broad handlers.',
		},
		messages: {
			taggedError: 'Use Effect.catchTag or Effect.catchTags instead of manually checking an error `_tag`.',
			taggedReason:
				'Use Effect.catchReason or Effect.catchReasons instead of manually checking a tagged error reason.',
		},
	},
	createOnce(context) {
		return {
			CallExpression(node) {
				if (!isEffectBroadCatch(node)) return
				for (const argument of node.arguments) {
					if (argument.type !== 'ArrowFunctionExpression' && argument.type !== 'FunctionExpression') continue
					const parameter = argument.params[0]
					if (parameter?.type !== 'Identifier') continue
					const body = argument.body
					const text = context.sourceCode.getText(body)
					const predicatePrefix = `Predicate.isTagged(${parameter.name}`
					if (text.includes(predicatePrefix)) {
						context.report({
							node: body,
							messageId: text.includes(`${predicatePrefix}.reason,`) ? 'taggedReason' : 'taggedError',
						})
						continue
					}
				}
			},
		}
	},
})
